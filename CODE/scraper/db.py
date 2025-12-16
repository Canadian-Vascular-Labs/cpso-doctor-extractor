# db_stream.py
import os
import json
import asyncio
from random import random
import time
import math
from typing import List, Dict, Any, Tuple
import aiomysql

# canonical column order - must match your MySQL table columns
COLUMNS = [
    "cpsonumber",
    "name",
    "specialties",
    "primaryaddressnotinpractice",
    "street1",
    "city",
    "province",
    "postalcode",
    "street2",
    "street3",
    "street4",
    "additionaladdresscount",
    "phonenumber",
    "fax",
    "registrationstatus",
    "mostrecentformername",
    "registrationstatuslabel",
    "additionaladdresses",
]

class DBStreamClient:
    def __init__(
        self,
        host: str,
        port: int,
        user: str,
        password: str,
        db: str,
        table: str = "doctors",
        minsize: int = 1,
        maxsize: int = 10,
        batch_size: int = 200,
        flush_interval: float = 3.0,
        max_retry: int = 5,
        buffer_dir: str = "db_buffer",
    ):
        self.dsn = dict(host=host, port=port, user=user, password=password, db=db)
        self.table = table
        self.minsize = minsize
        self.maxsize = maxsize
        self.batch_size = batch_size
        self.flush_interval = flush_interval
        self.max_retry = max_retry
        self.buffer_dir = buffer_dir

        self.pool: aiomysql.Pool | None = None
        self.queue: asyncio.Queue = asyncio.Queue()
        self._worker_task: asyncio.Task | None = None
        self._closing = False

        os.makedirs(self.buffer_dir, exist_ok=True)

    async def connect(self):
        self.pool = await aiomysql.create_pool(
            minsize=self.minsize,
            maxsize=self.maxsize,
            autocommit=True,
            **self.dsn,
        )
        # start background writer task
        self._worker_task = asyncio.create_task(self._writer())

    async def enqueue_records(self, records: List[Dict[str, Any]]):
        """
        Called by scraper. Provide a list of record dicts (can be any length).
        They are queued for background write into MySQL.
        """
        if not records:
            return
        await self.queue.put(records)

    async def _writer(self):
        """
        Background worker: batches items from queue and writes to DB.
        Flushes either when batch_size is reached or flush_interval elapses.
        """
        pending: List[Dict[str, Any]] = []
        last_flush = time.monotonic()
        while True:
            try:
                # wait for the next item with timeout to enforce periodic flush
                try:
                    item = await asyncio.wait_for(self.queue.get(), timeout=self.flush_interval)
                    pending.extend(item)
                    self.queue.task_done()
                except asyncio.TimeoutError:
                    pass

                # flush when enough items collected or interval elapsed
                now = time.monotonic()
                if pending and (len(pending) >= self.batch_size or (now - last_flush) >= self.flush_interval):
                    batch, pending = pending[: self.batch_size], pending[self.batch_size :]
                    await self._flush_batch_with_retry(batch)
                    last_flush = time.monotonic()

                # if shutdown and queue empty and no pending -> break
                if self._closing and self.queue.empty() and not pending:
                    break

            except Exception as e:
                # log and sleep briefly; keep worker alive
                print(f"[dbstream] writer loop error: {e}")
                await asyncio.sleep(1.0)

        # final flush remaining pending
        if pending:
            await self._flush_batch_with_retry(pending)

    async def _flush_batch_with_retry(self, batch: List[Dict[str, Any]]):
        """
        Attempts to write a batch to DB with retry/backoff. On repeated failure,
        save batch to disk buffer for manual replay later.
        """
        attempt = 0
        backoff = 1.0
        while attempt < self.max_retry:
            try:
                await self._write_batch(batch)
                return
            except Exception as e:
                attempt += 1
                print(f"[dbstream] DB write attempt {attempt} failed: {e}")
                await asyncio.sleep(backoff + random.random() * 0.5)
                backoff = min(backoff * 2, 30.0)

        # after retries, fallback to disk buffer
        self._save_batch_to_disk(batch)
        print(f"[dbstream] Saved batch to disk buffer after {self.max_retry} failures")

    async def _write_batch(self, records: List[Dict[str, Any]]):
        """
        Transform records into tuples ordered by COLUMNS and run executemany with ON DUPLICATE KEY UPDATE.
        """
        if not self.pool:
            raise RuntimeError("DB pool not initialized")

        keys = COLUMNS
        cols_sql = ", ".join(keys)
        
        placeholders = ", ".join(["%s"] * len(keys))
        alias = "new"

        updates = ", ".join(
            f"{k}={alias}.{k}"
            for k in keys
            if k != "cpsonumber"
        )

        sql = (
            f"INSERT INTO {self.table} ({cols_sql}) "
            f"VALUES ({placeholders}) AS {alias} "
            f"ON DUPLICATE KEY UPDATE {updates}"
        )
        
        values = []
        for r in records:
            row = []
            for k in keys:
                v = r.get(k)
                if v is None:
                    # sensible defaults by column type
                    if k == "additionaladdresses":
                        v = json.dumps([])
                    elif k == "additionaladdresscount":
                        v = 0
                    elif k == "primaryaddressnotinpractice":
                        v = False
                    else:
                        v = ""
                # ensure additionaladdresses stored as JSON string
                if k == "additionaladdresses" and not isinstance(v, str):
                    v = json.dumps(v)
                row.append(v)
            values.append(tuple(row))

        # chunk large writes to avoid memory/packet limits
        CHUNK = 500
        async with self.pool.acquire() as conn:
            async with conn.cursor() as cur:
                for i in range(0, len(values), CHUNK):
                    seg = values[i : i + CHUNK]
                    await cur.executemany(sql, seg)

    def _save_batch_to_disk(self, batch: List[Dict[str, Any]]):
        fname = f"batch_{int(time.time())}_{math.floor(random()*100000)}.json"
        path = os.path.join(self.buffer_dir, fname)
        with open(path, "w", encoding="utf8") as f:
            json.dump(batch, f, indent=2, ensure_ascii=False)

    async def replay_disk_buffer(self, remove_after_success: bool = True):
        """
        Try to replay any files in buffer_dir back into DB.
        """
        files = sorted(os.listdir(self.buffer_dir))
        for fn in files:
            path = os.path.join(self.buffer_dir, fn)
            try:
                with open(path, "r", encoding="utf8") as f:
                    batch = json.load(f)
                await self._flush_batch_with_retry(batch)
                if remove_after_success:
                    os.remove(path)
            except Exception as e:
                print(f"[dbstream] replay failed for {fn}: {e}")

    async def close(self):
        """
        Graceful shutdown: stop accepting new items, wait queue drained, stop worker.
        """
        self._closing = True
        # wait until queue empty
        await self.queue.join()
        # wait worker to exit
        if self._worker_task:
            await self._worker_task
        # close pool
        if self.pool:
            self.pool.close()
            await self.pool.wait_closed()
