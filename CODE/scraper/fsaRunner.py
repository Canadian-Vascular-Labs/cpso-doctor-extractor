# scraper.py
import asyncio
import httpx
from datetime import datetime

from fetch import fetch_one
from fanout import build_param_sets


def log(msg: str):
    ts = datetime.now().strftime("%H:%M:%S")
    print(f"[{ts}] {msg}")


class FSARunner:
    """
    Runs a single FSA end-to-end:
      - isolated queue
      - local workers
      - all fanout contained
    """

    def __init__(
        self,
        fsa: str,
        client: httpx.AsyncClient,
        sem: asyncio.Semaphore,
        base_url: str,
        db,
        specialties: list[str],
        concurrency: int,
        ldus: list[str] = [],
    ):
        self.fsa = fsa
        self.ldus = ldus
        self.client = client
        self.sem = sem
        self.base_url = base_url
        self.db = db
        self.specialties = specialties
        self.concurrency = concurrency

        self.queue: asyncio.Queue = asyncio.Queue()
        self.seen: set[int] = set()
        self.seen_lock = asyncio.Lock()

        self.active_workers = 0
        self.request_count = 0

    # -------------------------
    async def run(self, fsa: bool = True):
        if fsa:
            initial_params = {
                "postalCode": self.fsa,
                # "cbx-includeinactive": "on",
            }
            await self.queue.put(
                (self.fsa, 0, initial_params)
            )  # default generic scrape

        stop_event = asyncio.Event()
        workers = [
            asyncio.create_task(self.worker(i, stop_event))
            for i in range(self.concurrency)
        ]

        await self.queue.join()
        stop_event.set()

        for w in workers:
            w.cancel()
        await asyncio.gather(*workers, return_exceptions=True)

        log(f"[FSA {self.fsa}] DONE | requests={self.request_count}")

    # -------------------------
    async def worker(self, wid: int, stop_event: asyncio.Event):
        while not stop_event.is_set():
            try:
                postal, fan_level, params = await asyncio.wait_for(
                    self.queue.get(), timeout=1.0
                )
            except asyncio.TimeoutError:
                continue

            self.active_workers += 1
            try:
                log(
                    f"[FSA {self.fsa} | worker {wid}] "
                    f"dequeue level={fan_level} params={params}"
                )
                await self.handle_task(postal, fan_level, params, wid)
            finally:
                self.queue.task_done()
                self.active_workers -= 1

    # -------------------------
    async def handle_task(self, postal, fan_level, params, wid: int):
        # if params is empty, set postal
        response = await fetch_one(
            self.client,
            self.sem,
            self.base_url,
            params,
            label=f"{self.fsa}|{fan_level}",
        )

        if not response:
            return

        self.request_count += 1
        total = response.get("totalcount")
        results = response.get("results") or []

        log(f"[FSA {self.fsa} | worker {wid}] " f"total={total} results={len(results)}")

        if total == -1:
            await self.fan_out(postal, fan_level, params)
            return

        for r in results:
            await self.handle_result(r)

    # -------------------------
    async def fan_out(self, postal, fan_level, params):
        next_level = fan_level + 1
        if next_level > 5:
            log(f"[FSA {self.fsa}] MAX FANOUT level reached")
            return

        for p in build_param_sets(
            postal,
            self.ldus,
            self.specialties,
            next_level,
            params,
        ):
            log(f"[FSA {self.fsa}] fanout {fan_level}->{next_level} " f"params={p}")
            await self.queue.put((postal, next_level, p))

    # -------------------------
    async def handle_result(self, res: dict):
        cpsonum = int(res.get("cpsonumber"))
        if not cpsonum:
            return

        async with self.seen_lock:
            if cpsonum in self.db.existing_cpsos:
                return
            if cpsonum in self.seen:
                return
            self.seen.add(cpsonum)

        record = {
            "cpsonumber": cpsonum,
            "name": res.get("name", ""),
            "mostrecentformername": res.get("mostrecentformername", ""),
            "registrationstatus": res.get("registrationstatus", ""),
            "primaryaddressnotinpractice": bool(res.get("primaryaddressnotinpractice")),
            "street1": res.get("street1", ""),
            "street2": res.get("street2", ""),
            "street3": res.get("street3", ""),
            "street4": res.get("street4", ""),
            "city": res.get("city", ""),
            "province": res.get("province", ""),
            "postalcode": res.get("postalcode", ""),
            "phonenumber": res.get("phonenumber", ""),
            "fax": res.get("fax", ""),
            "specialties": self.normalize_specialties(res.get("specialties")),
        }

        await self.db.enqueue_records([record])

    # -------------------------
    def normalize_specialties(self, raw):
        if not raw:
            return []
        if isinstance(raw, list):
            return sorted({s.strip() for s in raw if s})
        return sorted({s.strip() for s in raw.split("|") if s})
