# db.py
import asyncio
import aiomysql
import time


class DBStreamClient:
    def __init__(
        self,
        host: str,
        port: int,
        user: str,
        password: str,
        db: str,
        batch_size: int = 200,
        flush_interval: float = 2.0,
    ):
        self.dsn = dict(
            host=host,
            port=port,
            user=user,
            password=password,
            db=db,
            autocommit=True,
        )

        self.batch_size = batch_size
        self.flush_interval = flush_interval

        self.pool = None
        self.queue = asyncio.Queue()
        self._worker = None
        self._closing = False

        self.existing_cpsos: set[int] = set()

    # -------------------------
    async def connect(self):
        self.pool = await aiomysql.create_pool(minsize=1, maxsize=10, **self.dsn)
        self._worker = asyncio.create_task(self._writer())

    async def preload_cpsos(self):
        async with self.pool.acquire() as conn:
            async with conn.cursor() as cur:
                await cur.execute("SELECT cpsonumber FROM doctors")
                rows = await cur.fetchall()
                self.existing_cpsos = {r[0] for r in rows}
        print(f"[db] preloaded {len(self.existing_cpsos):,} CPSOs")

    async def close(self):
        self._closing = True
        await self.queue.put([])
        await self.queue.join()
        if self._worker:
            await self._worker
        self.pool.close()
        await self.pool.wait_closed()

    # -------------------------
    async def enqueue_records(self, records):
        if records:
            await self.queue.put(records)

    # -------------------------
    async def _writer(self):
        pending = []
        last_flush = time.monotonic()

        while True:
            try:
                try:
                    batch = await asyncio.wait_for(
                        self.queue.get(), timeout=self.flush_interval
                    )
                    pending.extend(batch)
                    self.queue.task_done()
                except asyncio.TimeoutError:
                    pass

                now = time.monotonic()
                if pending and (
                    len(pending) >= self.batch_size
                    or now - last_flush >= self.flush_interval
                ):
                    await self._flush(pending)
                    pending.clear()
                    last_flush = now

                if self._closing and self.queue.empty():
                    break

            except Exception as e:
                print("[db] writer error:", e)
                await asyncio.sleep(1)

        if pending:
            await self._flush(pending)

    # -------------------------
    async def _flush(self, records):
        async with self.pool.acquire() as conn:
            async with conn.cursor() as cur:
                for r in records:
                    await self._write_doctor(cur, r)
                    await self._write_addresses(cur, r)
                    await self._write_specialties(cur, r)
                    self.existing_cpsos.add(r["cpsonumber"])  # ✅ after success

    # -------------------------
    async def _write_doctor(self, cur, r):
        await cur.execute(
            """
            INSERT INTO doctors (
                cpsonumber, name, mostrecentformername,
                registrationstatus, primaryaddressnotinpractice
            )
            VALUES (%s,%s,%s,%s,%s) AS new
            ON DUPLICATE KEY UPDATE
                name=new.name,
                mostrecentformername=new.mostrecentformername,
                registrationstatus=new.registrationstatus,
                primaryaddressnotinpractice=new.primaryaddressnotinpractice
            """,
            (
                r["cpsonumber"],
                r["name"],
                r["mostrecentformername"],
                r["registrationstatus"],
                r["primaryaddressnotinpractice"],
            ),
        )

    async def _write_addresses(self, cur, r):
        await cur.execute(
            """
            INSERT IGNORE INTO addresses (
                cpsonumber, street1, street2, street3, street4,
                city, province, postalcode, phonenumber, fax, is_primary
            )
            VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
            """,
            (
                r["cpsonumber"],
                r["street1"],
                r["street2"],
                r["street3"],
                r["street4"],
                r["city"],
                r["province"],
                r["postalcode"],
                r["phonenumber"],
                r["fax"],
                True,
            ),
        )

    async def _write_specialties(self, cur, r):
        for name in r["specialties"]:
            await cur.execute(
                "INSERT IGNORE INTO specialties (name) VALUES (%s)",
                (name,),
            )
            await cur.execute(
                """
                INSERT IGNORE INTO doctor_specialties (cpsonumber, specialty_id)
                SELECT %s, id FROM specialties WHERE name=%s
                """,
                (r["cpsonumber"], name),
            )
