import asyncio
from pymongo import MongoClient
import random
import json
import re
import os
from collections import deque
from string import ascii_uppercase
import time

import aiofiles
import httpx
import demjson3
from rich.progress import Progress, SpinnerColumn, BarColumn, TextColumn
from rich.console import Console

# Local imports
from data.generate_postal_codes_to_json import group_postal_codes
from helpers import PhysicianScraper, clean_number


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
        """
        :param concurrency: Number of concurrent requests to the CPSO API
        :param phys_concurrency: Number of concurrent requests to the physician scraper (for additional addresses)
        """
        headers = {
            "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
            "AppleWebKit/537.36 (KHTML, like Gecko) "
            "Chrome/114.0.0.0 Safari/537.36",
            "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
            "Accept-Language": "en-US,en;q=0.5",
            "Referer": "https://register.cpso.on.ca/",
        }

        self.sem = asyncio.Semaphore(concurrency)
        self.phys_sem = asyncio.Semaphore(phys_concurrency)

        # HTTP clients
        self.timeout = timeout or httpx.Timeout(5.0, read=15.0, write=5.0, pool=5.0)
        self.client = httpx.AsyncClient(
            timeout=self.timeout,
            headers=headers,
            follow_redirects=True,
        )

        # State
        self.extracted = set()
        self.skipped = set()

        # Logging
        self.console = Console()

        # Physician scraper for additional addresses
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
        raw = re.sub(r",\s*}", "}", raw)
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
                    response = await self.client.get(self.BASE_URL, params=params)
                    response.raise_for_status()
                    text = response.text
                    try:
                        data = json.loads(text)
                    except json.JSONDecodeError:
                        cleaned = self.repair_broken_json(text)
                        data = demjson3.decode(cleaned)
                    # brief pause to avoid hammering the server
                    await asyncio.sleep(0.1 + random.random() * 0.1)
                    return 1, data
                except httpx.ConnectTimeout:
                    self.console.log(
                        f"[yellow]Connect timeout {postal} attempt {attempt}/8"
                    )
                except (httpx.ReadTimeout, httpx.ReadError) as e:
                    self.console.log(
                        f"[yellow]{postal} read error: {e} attempt {attempt}/8"
                    )
                except httpx.HTTPStatusError as he:
                    status = he.response.status_code
                    url = he.request.url
                    self.console.log(f"[yellow]{postal} → HTTP {status} for {url}")
                except httpx.HTTPError as e:
                    self.console.log(f"[yellow]{postal} → Network error: {e}")
                except Exception as e:
                    self.console.log(f"[red]Unexpected error {postal}: {e}")
                await asyncio.sleep(backoff + random.random() * 0.5)
                backoff = min(backoff * 2, 10.0)
        return 0, {}

    async def _fetch_and_attach(self, res: dict):
        # Controlled by phys_sem
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

        while queue:
            params = queue.popleft()
            status, data = await self.fetch_one(params, postal)
            if status != 1:
                continue
            total = data.get("totalcount", 0)

            if total == -1 and params.get("doctorType"):
                for letter in ascii_uppercase:
                    newp = params.copy()
                    newp["firstName"] += letter
                    queue.append(newp)
                continue

            formattedPostal = postal.strip().upper()
            for res in data.get("results", []):
                if (
                    res.get("postalcode", "").strip().upper() == formattedPostal
                    and res.get("cpsonumber") not in self.extracted
                ):
                    self.extracted.add(res.get("cpsonumber"))
                    # clean phone/fax to digits only
                    if res.get("phonenumber"):
                        res["phonenumber"] = clean_number(res.get("phonenumber"))
                    if res.get("fax"):
                        res["fax"] = clean_number(res.get("fax"))
                    # schedule additional address fetch
                    if res.get("additionaladdresscount", 0) > 0:
                        phys_tasks.append(
                            asyncio.create_task(self._fetch_and_attach(res))
                        )
                    else:
                        res["additionaladdresses"] = []
                    results.append(res)

        # run all phys scraping concurrently
        if phys_tasks:
            await asyncio.gather(*phys_tasks)

        return postal, results

    @staticmethod
    async def serialize(batch: list[dict]) -> str:
        return await asyncio.to_thread(json.dumps, batch, indent=2)

    async def flush_to_disk(self, postal: str, batch: list[dict]):
        """
        Writes or updates <FSA>.json mapping full postal to its batch list.
        """
        fsa = postal.split()[0].lower()
        path = os.path.join("extracted_data", f"{fsa}.json")
        # load existing
        try:
            async with aiofiles.open(path, "r") as f:
                content = await f.read()
                data = json.loads(content)
        except (FileNotFoundError, json.JSONDecodeError):
            data = {}
        # update mapping
        data[postal] = batch
        serialized = await self.serialize(data)
        async with aiofiles.open(path, "w") as f:
            await f.write(serialized)

    async def run(self):
        os.makedirs("extracted_data", exist_ok=True)

        # load inputs
        if not os.path.exists("data/grouped_postal_codes.json"):
            group_postal_codes("data/grouped_postal_codes.json")
        with open("data/grouped_postal_codes.json") as f:
            grouped = json.load(f)
        with open("data/specialties.json") as f:
            specs = json.load(f)["specialties"]

        postals = [f"{fsa} {ldu}" for fsa, ldus in grouped.items() for ldu in ldus]
        total = len(postals)

        progress = Progress(
            SpinnerColumn(),
            TextColumn("{task.description}"),
            BarColumn(),
            TextColumn("{task.completed}/{task.total}"),
            console=self.console,
        )
        with progress as prog:
            task_id = prog.add_task("Scraping", total=total)
            tasks = []
            for postal in postals:
                tasks.append(
                    asyncio.create_task(
                        self._handle_postal(postal, specs, prog, task_id)
                    )
                )
            await asyncio.gather(*tasks)

        await self.client.aclose()
        self.console.log(
            f"[green]Done. Extracted {len(self.extracted)}, skipped {len(self.skipped)}"
        )

    async def _handle_postal(self, postal, specs, progress, task_id):
        postal, results = await self.process_postal(postal, specs)
        await self.flush_to_disk(postal, results)
        progress.update(task_id, advance=1, description=f"Done {postal}")
        self.console.log(f"[green]Finished {postal}: {len(results)}")


class DBClient:
    """
    MongoDB client using PyMongo; automatically picks DB from URI or env var.
    """

    def __init__(self):
        # Load .env if present
        if os.path.exists(".env"):
            from dotenv import load_dotenv

            load_dotenv()

        node_env = os.getenv("NODE_ENV", "development")
        uri = (
            os.getenv("MONGO_URI_PROD")
            if node_env == "production"
            else os.getenv("MONGO_URI_DEV")
        )
        if not uri:
            raise RuntimeError("MONGO_URI_DEV or MONGO_URI_PROD must be set")

        # Initialize PyMongo client
        self.client = MongoClient(uri)
        db_name = os.getenv("MONGO_DB_NAME")
        self.db = (
            self.client[db_name] if db_name else self.client.get_default_database()
        )

        coll_name = os.getenv("MONGO_COLLECTION", "doctors")
        self.collection = self.db[coll_name]

    # update records in MongoDB by CPSO number
    async def update_records(self, records: list[dict]):
        """
        Batch upsert multiple physician records using bulk_write.
        """
        from pymongo import UpdateOne

        operations = []
        for rec in records:
            key = {"cpsonumber": rec.get("cpsonumber")}
            operations.append(UpdateOne(key, {"$set": rec}, upsert=True))
        if operations:
            await asyncio.to_thread(
                self.collection.bulk_write, operations, ordered=False
            )

    async def close(self):
        # Close the client in a background thread
        await asyncio.to_thread(self.client.close)

    async def run(self):
        total_updated = 0

        for file in os.listdir("extracted_data"):
            if not file.endswith(".json"):
                continue

            fsa = file.split(".")[0].upper()
            with open(os.path.join("extracted_data", file)) as f:
                data = json.load(f)

            records = []
            for postal, recs in data.items():
                records.extend(recs)

            if records:
                await self.update_records(records)
                print(f"Updated {len(records)} records for {fsa}")
                total_updated += len(records)

        await self.close()
        return total_updated


if __name__ == "__main__":
    scraper = CPSOScraper(concurrency=5)
    # # track script execution time
    start_time = time.time()
    asyncio.run(scraper.run())
    end_time = time.time()
    elapsed_time = end_time - start_time
    print(f"Extraction executed in {elapsed_time:.2f} seconds")

    # need to interact with DB to update the records
    db_client = DBClient()

    # track script execution time
    start_time = time.time()
    total_updated = asyncio.run(db_client.run())
    print(f"Updated {total_updated} records in MongoDB")

    end_time = time.time()
    elapsed_time = end_time - start_time
    print(f"DB update executed in {elapsed_time:.2f} seconds")
