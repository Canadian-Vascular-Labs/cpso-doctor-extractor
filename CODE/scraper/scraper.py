# scraper.py
import asyncio
import httpx

from fetch import fetch_one
from fanout import build_param_sets
from state import ScraperState
from utils import generate_valid_ontario_fsas


class CPSOScraper:
    def __init__(self, concurrency: int, base_url: str, db):
        self.base_url = base_url
        self.sem = asyncio.Semaphore(concurrency)
        self.db = db

        self.queue = asyncio.Queue()
        self.state = ScraperState()

        self.specialties = []  # load from file / constants if needed

        self.extracted = set()  # CPSO numbers
        self.extracted_lock = asyncio.Lock()

        self.request_count = 0
        self.start_time = None
        self.active_workers = 0


    # -------------------------
    # public entrypoint
    # -------------------------
    async def run(self, stop_event: asyncio.Event):
        self.start_time = asyncio.get_event_loop().time()
        reporter_task = asyncio.create_task(self.reporter(stop_event))

        async with httpx.AsyncClient(timeout=30) as client:
            self.client = client

            await self._seed_queue()

            workers = [
                asyncio.create_task(self.worker(i, stop_event))
                for i in range(self.sem._value)
            ]
            await self.queue.join()
            stop_event.set()

            # cancel workers
            for w in workers:
                w.cancel()
            reporter_task.cancel()

            await asyncio.gather(*workers, reporter_task, return_exceptions=True)


    # -------------------------
    # queue seeding
    # -------------------------
    async def _seed_queue(self):
        # fsas = generate_valid_ontario_fsas()

        # for fsa in fsas:
        #     # start at fan_level 0
        #     await self.queue.put((fsa, 0, {}))
        # print(f"[+] Seeded {len(fsas)} FSAs")
        await self.queue.put(("K1K 0T2", 1, {}))


    # -------------------------
    # worker loop
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
                await self.handle_task(postal, fan_level, params)
            finally:
                self.queue.task_done()
                self.active_workers -= 1

    # -------------------------
    # core task logic
    # -------------------------
    async def handle_task(self, postal, fan_level, params):
        if self.state.seen(postal, params):
            return

        response = await fetch_one(
            self.client,
            self.sem,
            self.base_url,
            params,
            label=f"{postal}|{fan_level}",
        )

        if not response:
            return

        total = response.get("totalcount")
        # after every successful fetch
        self.request_count += 1

        # CPSO signals too many results
        if total == -1:
            await self.fan_out(postal, fan_level, params)
            return
        


        results = response.get("results") or []
        for r in results:
            await self.handle_result(r)

    # -------------------------
    # fan logic
    # -------------------------
    async def fan_out(self, postal, fan_level, params):
        next_level = fan_level + 1

        for new_params in build_param_sets(
            postal,
            self.specialties,
            next_level,
            params,
        ):
            print(
                f"[fan] {postal} | level={fan_level} → {next_level} "
                f"| params={list(new_params.items())}"
            )
            await self.queue.put((postal, next_level, new_params))

    # -------------------------
    # result handling
    # -------------------------
    async def handle_result(self, res: dict):
        cpsonum = res.get("cpsonumber")
        if not cpsonum:
            return

        async with self.extracted_lock:
            if cpsonum in self.extracted:
                return
            self.extracted.add(cpsonum)

        # write to DB 
        record = {
            "cpsonumber": cpsonum,
            "name": res.get("name", ""),
            "specialties": res.get("specialties", ""),
            "street1": res.get("street1", ""),
            "city": res.get("city", ""),
            "province": res.get("province", ""),
            "postalcode": res.get("postalcode", ""),
            "phonenumber": res.get("phonenumber", ""),
            "registrationstatus": res.get("registrationstatus", ""),
            "additionaladdresses": res.get("additionaladdresses", []),
            # etc — map once here
        }
        # 🔥 async, non-blocking, safe
        await self.db.enqueue_records([record])



    async def reporter(self, stop_event: asyncio.Event):
        while not stop_event.is_set():
            await asyncio.sleep(5)

            elapsed = asyncio.get_event_loop().time() - self.start_time
            rps = self.request_count / elapsed if elapsed else 0

            print(
                f"[status] extracted={len(self.extracted):,} "
                f"| queue={self.queue.qsize():,} "
                f"| active={self.active_workers} "
                f"| req/s={rps:.1f}"
            )
