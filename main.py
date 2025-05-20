import asyncio
import uvloop
import shutil
import random
import json
import re
import os
import time
from collections import deque
from string import ascii_uppercase

import aiofiles
import httpx
import demjson3
from rich.progress import Progress, SpinnerColumn, BarColumn, TextColumn
from rich.console import Console

# Local imports
from data.generate_postal_codes_to_json import group_postal_codes
from helpers import PhysicianScraper, clean_number
from pymongo import MongoClient, UpdateOne
from dotenv import load_dotenv

# global variables
postals = []
specs = []


# Load environment variables
def load_env():
    if os.path.exists(".env"):
        load_dotenv()


class CPSOScraper:
    BASE_URL = "https://register.cpso.on.ca/Get-Search-Results/"
    ADDR_FIELDS = [
        "street1",
        "street2",
        "street3",
        "street4",
        "city",
        "province",
        "postalcode",
    ]

    def __init__(
        self,
        concurrency: int = 100,
        timeout: httpx.Timeout = None,
    ):
        self.console = Console()

        self.load_postals()
        concurrency = max(concurrency, len(postals))
        self.concurrency = concurrency
        self.console.log(f"Concurrency set to {concurrency}")

        self.sem = asyncio.Semaphore(concurrency)
        self.timeout = timeout or httpx.Timeout(2.0, read=15.0, write=5.0, pool=5.0)
        headers = {
            "User-Agent": (
                "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
                "AppleWebKit/537.36 (KHTML, like Gecko) "
                "Chrome/114.0.0.0 Safari/537.36"
            ),
            "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
            "Accept-Language": "en-US,en;q=0.5",
            "Referer": "https://register.cpso.on.ca/",
        }
        # Enable HTTP/2 multiplexing
        self.client = httpx.AsyncClient(
            http2=True,
            timeout=self.timeout,
            headers=headers,
            follow_redirects=True,
            limits=httpx.Limits(
                max_connections=concurrency,
                max_keepalive_connections=concurrency,
            ),
        )
        self.extracted = set()
        self.failed_postals = set()

        # store this as a dictionary with FSA as key, CPSO numbers as value
        self.phys_with_additional_addrs = dict()
        self.skipped_fans = dict()
        self._file_locks: dict[str, asyncio.Lock] = {}

    # Load postal codes from file
    @staticmethod
    def load_postals() -> list[str]:
        global postals, specs

        # prepare storage
        if os.path.exists("extracted_data"):
            shutil.rmtree("extracted_data")
        os.makedirs("extracted_data", exist_ok=True)
        # prepare inputs
        if not os.path.exists("data/grouped_postal_codes.json"):
            group_postal_codes("data/grouped_postal_codes.json")
        with open("data/grouped_postal_codes.json") as f:
            grouped = json.load(f)
        with open("data/specialties.json") as f:
            specs = json.load(f)["specialties"]
        postals = [f"{fsa} {ldu}" for fsa, ldus in grouped.items() for ldu in ldus]

    @staticmethod
    def repair_broken_json(raw: str) -> str:
        raw = re.sub(r"[\x00-\x1F\x7F]", "", raw)
        raw = raw.replace('\\"', '"')
        raw = re.sub(r'(?<!\\)\\(?![\\nt"r/])', r"\\\\", raw)
        raw = re.sub(r'(" : [^,}\]]+)(\s*)"(\w+)":', r'\1,\2"\3":', raw)
        raw = re.sub(r",\s*,+", ",", raw)
        raw = re.sub(r"{\s*,", "{", raw)
        raw = re.sub(r"\[\s*,", "[", raw)
        raw = re.sub(r"{\s*,", "}", raw)
        raw = re.sub(r",\s*]", "]", raw)
        return raw

    @staticmethod
    def build_param_sets(postal: str, specialties: list[str]):
        yield {
            "cbx-includeinactive": "on",
            "postalCode": postal,
            "doctorType": "Family Doctor",
            "firstName": "",
        }
        for spec in specialties:
            yield {
                "cbx-includeinactive": "on",
                "postalCode": postal,
                "doctorType": "Specialist",
                "SpecialistType": spec,
                "firstName": "",
            }

    async def fetch_one(self, params: dict, postal: str) -> tuple[int, dict]:
        async with self.sem:
            backoff = 1.0
            for attempt in range(1, 9):
                try:
                    resp = await self.client.get(self.BASE_URL, params=params)
                    resp.raise_for_status()
                    text = resp.text
                    try:
                        data = json.loads(text)
                    except json.JSONDecodeError:
                        raw = self.repair_broken_json(text)
                        data = demjson3.decode(raw)
                    return 1, data
                except httpx.HTTPStatusError as he:
                    self.console.log(
                        f"[yellow]{postal} → HTTP {he.response.status_code}"
                    )
                except httpx.HTTPError as e:
                    self.console.log(f"[yellow]{postal} → Network error: {e}")
                await asyncio.sleep(backoff + random.random() * 0.5)
                backoff = min(backoff * 2, 10.0)
        return 0, {}

    def _process_result(self, res: dict, results: list, FSA: str):
        cpsonum = res.get("cpsonumber")
        if not cpsonum or cpsonum in self.extracted:
            return
        if res.get("phonenumber"):
            res["phonenumber"] = clean_number(res.get("phonenumber"))
        if res.get("fax"):
            res["fax"] = clean_number(res.get("fax"))
        if res.get("postalcode"):
            res["postalcode"] = res.get("postalcode").strip().upper()

        if res.get("additionaladdresscount", 0) > 0:
            self.phys_with_additional_addrs.setdefault(FSA, []).append(cpsonum)
        else:
            res["additionaladdresses"] = []

        self.extracted.add(cpsonum)
        results.append(res)

    async def process_postal(
        self,
        async_queue: asyncio.Queue,
        progress: Progress,
        postal: str,
        params: dict,
        specialties: list[str],
    ) -> tuple[str, list[dict]]:
        results = []

        formatted_postal = postal.strip().upper()

        # self.console.log(f"[green]Processing {postal} with params {params}")

        status, data = await self.fetch_one(params, formatted_postal)
        if status != 1:
            self.console.log(f"[red]Failed to fetch {postal}")
            self.failed_postals.add(postal)

        total = data.get("totalcount", 0)
        # self.console.log(f"[cyan]Fetched {postal} for {params} with {total} results")
        if total == -1:
            # self.console.log(
            #     f"[red]Skipping {postal} due to too many results ({total})"
            # )
            queue = deque()

            # build params for fanned postals
            if not params.get("doctorType"):
                queue = deque(self.build_param_sets(postal, specialties))
            else:
                # fan with ASCII for first name
                for letter in ascii_uppercase:
                    newp = params.copy()
                    newp["firstName"] += letter
                    queue.append(newp)

            for params in queue:
                # self.console.log(f"[yellow]Fanning {postal} with params {params}")
                async_queue.put_nowait((postal, params))
                self.total += 1
                progress.update(self.task_id, total=self.total)

            # exit(0)

        for res in data.get("results", []):
            if res.get("postalcode", "").strip().upper() == formatted_postal:
                FSA = formatted_postal[:3]
                self._process_result(res, results, FSA)
        # if not params.get("doctorType") and total >= 0:
        #     break

        return postal, results

    async def flush_to_disk(self, postal: str, batch: list[dict]):
        fsa = postal.split()[0].upper()
        path = os.path.join("extracted_data", f"{fsa}.json")

        # acquire lock for the file
        lock = self._file_locks.setdefault(path, asyncio.Lock())

        async with lock:
            try:
                async with aiofiles.open(path, "r") as f:
                    data = json.loads(await f.read())
            except (json.JSONDecodeError, FileNotFoundError, OSError):
                data = {}

            # store as dict of CPSO numbers { "cpsonumber": [record] }
            for record in batch:
                cpsonum = record.get("cpsonumber")
                if not cpsonum:
                    continue
                if cpsonum not in self.extracted:
                    self.console.log(f"[red]Missing CPSO Number: {cpsonum}")
                    continue
                data[cpsonum] = record

            serialized = json.dumps(data, indent=2)
            async with aiofiles.open(path, "w") as f:
                await f.write(serialized)

    async def _worker(
        self, queue: asyncio.Queue, specs: list[str], progress: Progress, task_id: int
    ):
        while True:
            postal, params = await queue.get()
            try:
                # self.console.log(f"[green]Processing {postal} with params {params}")
                postal, results = await self.process_postal(
                    queue, progress, postal, params, specs
                )
                # Optionally write to disk:
                await self.flush_to_disk(postal, results)
                progress.update(task_id, advance=1, description=f"Done {postal}")
            finally:
                queue.task_done()

    async def run(self):
        # queue + workers
        queue = asyncio.Queue()
        for p in postals:
            queue.put_nowait(
                (
                    p,
                    {
                        "cbx-includeinactive": "on",
                        "postalCode": p,
                        "firstName": "",
                    },
                )
            )

        with Progress(
            SpinnerColumn(),
            TextColumn("{task.description}"),
            BarColumn(),
            TextColumn("{task.completed}/{task.total}"),
            console=self.console,
        ) as prog:
            self.progress = prog
            task_id = prog.add_task("Scraping", total=queue.qsize())
            self.task_id = task_id
            self.total = queue.qsize()
            # launch a fixed number of workers
            workers = [
                asyncio.create_task(self._worker(queue, specs, prog, task_id))
                for _ in range(self.concurrency)
            ]
            await queue.join()
            for w in workers:
                w.cancel()

        await self.client.aclose()
        self.console.log(f"[green]Done. Extracted {len(self.extracted)} CPSONumbers")


class DBClient:
    def __init__(self):
        load_env()
        node_env = os.getenv("NODE_ENV", "development")
        uri = (
            os.getenv("MONGO_URI_PROD")
            if node_env == "production"
            else os.getenv("MONGO_URI_DEV")
        )
        if not uri:
            raise RuntimeError("Set MONGO_URI_DEV or _PROD")
        self.client = MongoClient(uri)
        db_name = os.getenv("MONGO_DB_NAME")
        self.db = (
            self.client[db_name] if db_name else self.client.get_default_database()
        )
        coll = os.getenv("MONGO_COLLECTION", "doctors")
        self.collection = self.db[coll]

    async def update_records(self, records: list[dict]):
        ops = [
            UpdateOne({"cpsonumber": r["cpsonumber"]}, {"$set": r}, upsert=True)
            for r in records
        ]
        if ops:
            await asyncio.to_thread(self.collection.bulk_write, ops, ordered=False)

    async def close(self):
        await asyncio.to_thread(self.client.close)

    async def run(self):
        total = 0
        for file in os.listdir("extracted_data"):
            if not file.endswith(".json"):
                continue
            print(f"Processing {file}")
            with open(os.path.join("extracted_data", file)) as f:
                data = json.load(f)
            # extract the values from the dict
            recs = [data[key] for key in data.keys()]
            if recs != []:
                await self.update_records(recs)
                print(f"Updated {len(recs)} for {file}")
                total += len(recs)
        await self.close()
        return total


async def main():
    load_env()

    scraper = CPSOScraper()
    start = time.time()
    await scraper.run()
    print(f"Scraped in {time.time() - start:.1f}s")

    # get total cpso numbers that have additional addresses
    total = sum(len(c) for c in scraper.phys_with_additional_addrs.values())
    print(f"Total CPSO Numbers with additional addresses: {total}")

    # delete the file if it exists
    if os.path.exists("additional_addresses.json"):
        os.remove("additional_addresses.json")
    # create a new file and save the dict of additional addresses to a json file
    with open("additional_addresses.json", "w") as f:
        json.dump(scraper.phys_with_additional_addrs, f, indent=2)

    """
    WORKFLOW FOR FANNING:
    
    """

    print(f"Skipped fanned postals length: {len(scraper.skipped_fans)}")
    # loop over postal:
    for postal in scraper.skipped_fans.keys():
        scraper.console.log(f"[green]{postal}: {scraper.skipped_fans[postal]}")

    """
    WORKFLOW FOR ADDITIONAL ADDRESSES:
    1. Get the FSA (key)
    2. Check if the CPSO number is already in the json file
    3. Query the additional addresses for the CPSO number, setting concurrency to total CPSO numbers with additional addresses
    4. Write the additional addresses to the json file 
    """
    # TEST WORKFLOW
    for fsa, cpsonum in scraper.phys_with_additional_addrs.items():
        path = os.path.join("extracted_data", f"{fsa.upper()}.json")
        if not os.path.exists(path):
            print(f"File does not exist: {path}")
            continue
        with open(path) as f:
            data = json.load(f)
        # make sure the CPSO number is in the json file
        CPSO_NUMS_IN_FSA = set()
        # the set is simply the keys of the json file
        for k in data.keys():
            CPSO_NUMS_IN_FSA.add(k)

        for c in cpsonum:
            if c not in CPSO_NUMS_IN_FSA:
                scraper.console.log(f"[red]Missing CPSO Number: {c} in {path}")

    # run the additional address scraper
    additional_addr_scraper = PhysicianScraper()
    start = time.time()
    await additional_addr_scraper.run()
    end = time.time()
    print(f"Scraped additional addresses in {end - start:.1f}s")

    scraper.console.log("[yellow] Updating DB")
    db = DBClient()
    start = time.time()
    tot = await db.run()
    print(f"Updated {tot} in {time.time() - start:.1f}s")


if __name__ == "__main__":
    uvloop.run(main())
