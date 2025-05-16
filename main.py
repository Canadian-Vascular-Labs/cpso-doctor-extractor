import asyncio
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

# Load environment variables
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
        concurrency: int = 5,
        phys_concurrency: int = 5,
        timeout: httpx.Timeout = None,
    ):
        self.sem = asyncio.Semaphore(concurrency)
        self.phys_sem = asyncio.Semaphore(phys_concurrency)
        self.timeout = timeout or httpx.Timeout(5.0, read=15.0, write=5.0, pool=5.0)
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
        self.client = httpx.AsyncClient(
            timeout=self.timeout,
            headers=headers,
            follow_redirects=True,
        )
        self.extracted = set()
        self.console = Console()
        self.phys_scraper = PhysicianScraper()

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
        yield {"cbx-includeinactive": "on", "postalCode": postal, "firstName": ""}
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
                        data = demjson3.decode(self.repair_broken_json(text))
                    await asyncio.sleep(0.1 + random.random() * 0.1)
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

    async def _fetch_and_attach(self, res: dict):
        async with self.phys_sem:
            cpsonum = res.get("cpsonumber")
            try:
                addrs = await asyncio.to_thread(self.phys_scraper.scrape, cpsonum)
            except Exception as e:
                self.console.log(f"[yellow]Failed phys scrape for {cpsonum}: {e}")
                addrs = []
            res["additionaladdresses"] = addrs

    async def process_postal(
        self, postal: str, specialties: list[str]
    ) -> tuple[str, list[dict]]:
        results = []
        queue = deque(self.build_param_sets(postal, specialties))
        phys_tasks = []
        formatted = postal.strip().upper()
        while queue:
            params = queue.popleft()
            status, data = await self.fetch_one(params, formatted)
            if status != 1:
                continue
            total = data.get("totalcount", 0)
            if not params.get("doctorType") and total >= 0 and data.get("results"):
                for res in data["results"]:
                    if res.get("postalcode", "").strip().upper() == formatted:
                        self._process_result(res, results, phys_tasks)
                break
            if total == -1 and params.get("doctorType"):
                for letter in ascii_uppercase:
                    newp = params.copy()
                    newp["firstName"] += letter
                    queue.append(newp)
                continue
            for res in data.get("results", []):
                if res.get("postalcode", "").strip().upper() == formatted:
                    self._process_result(res, results, phys_tasks)
        if phys_tasks:
            await asyncio.gather(*phys_tasks)
        return postal, results

    def _process_result(self, res: dict, results: list, phys_tasks: list):
        cpsonum = res.get("cpsonumber")
        if not cpsonum or cpsonum in self.extracted:
            return
        self.extracted.add(cpsonum)
        if res.get("phonenumber"):
            res["phonenumber"] = clean_number(res.get("phonenumber"))
        if res.get("fax"):
            res["fax"] = clean_number(res.get("fax"))
        if res.get("additionaladdresscount", 0) > 0:
            phys_tasks.append(asyncio.create_task(self._fetch_and_attach(res)))
        else:
            res["additionaladdresses"] = []
        results.append(res)

    @staticmethod
    async def serialize(obj: dict) -> str:
        return await asyncio.to_thread(json.dumps, obj, indent=2)

    async def flush_to_disk(self, postal: str, batch: list[dict]):
        fsa = postal.split()[0].lower()
        path = os.path.join("extracted_data", f"{fsa}.json")
        try:
            async with aiofiles.open(path, "r") as f:
                data = json.loads(await f.read())
        # catch all errors
        except (json.JSONDecodeError, FileNotFoundError, OSError):
            data = {}
        data[postal] = batch
        serialized = await self.serialize(data)
        async with aiofiles.open(path, "w") as f:
            await f.write(serialized)

    async def run(self):
        # ensure clean state
        if os.path.exists("extracted_data"):
            shutil.rmtree("extracted_data")
        os.makedirs("extracted_data", exist_ok=True)
        if not os.path.exists("data/grouped_postal_codes.json"):
            group_postal_codes("data/grouped_postal_codes.json")
        with open("data/grouped_postal_codes.json") as f:
            grouped = json.load(f)
        with open("data/specialties.json") as f:
            specs = json.load(f)["specialties"]
        postals = [f"{fsa} {ldu}" for fsa, ldus in grouped.items() for ldu in ldus]

        # progress + gather inside same context
        with Progress(
            SpinnerColumn(),
            TextColumn("{task.description}"),
            BarColumn(),
            TextColumn("{task.completed}/{task.total}"),
            console=self.console,
        ) as prog:
            task_id = prog.add_task("Scraping", total=len(postals))
            tasks = [
                asyncio.create_task(self._handle_postal(p, specs, prog, task_id))
                for p in postals
            ]
            await asyncio.gather(*tasks)

        await self.client.aclose()
        self.console.log(f"[green]Done. Extracted {len(self.extracted)} CPSONumbers")

    async def _handle_postal(self, postal, specs, progress, task_id):
        postal, results = await self.process_postal(postal, specs)
        await self.flush_to_disk(postal, results)
        progress.update(task_id, advance=1, description=f"Done {postal}")


class DBClient:
    def __init__(self):
        if os.path.exists(".env"):
            load_dotenv()
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
            with open(os.path.join("extracted_data", file)) as f:
                data = json.load(f)
            recs = [r for lst in data.values() for r in lst]
            if recs:
                await self.update_records(recs)
                print(f"Updated {len(recs)} for {file}")
                total += len(recs)
        await self.close()
        return total


async def main():
    scraper = CPSOScraper(concurrency=5)
    start = time.time()
    await scraper.run()
    print(f"Scraped in {time.time() - start:.1f}s")
    db = DBClient()
    start = time.time()
    tot = await db.run()
    print(f"Updated {tot} in {time.time() - start:.1f}s")


if __name__ == "__main__":
    asyncio.run(main())
