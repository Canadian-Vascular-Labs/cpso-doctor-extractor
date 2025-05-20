import re
import json
from itertools import zip_longest
from typing import List, Dict, Optional
import asyncio
import random
import httpx
import requests
from bs4 import BeautifulSoup
import demjson3
import uvloop
from rich.console import Console
import rich
from rich.progress import Progress, SpinnerColumn, BarColumn, TextColumn
import os


def clean_number(raw: str) -> str:
    """
    Remove non-digit characters and extract last 10 digits if available.
    """
    # strip unicode directionals and anything non-digit
    digits = re.sub(r"\D", "", raw)
    # return last 10 digits if longer, else full
    return digits[-10:] if len(digits) >= 10 else digits


class PhysicianScraper:
    """
    Scrapes CPSO physician address, phone, and fax details by CPSO number.
    """

    BASE_URL = "https://register.cpso.on.ca/physician-info/"

    _PROVINCES = {
        "AB": "Alberta",
        "BC": "British Columbia",
        "MB": "Manitoba",
        "NB": "New Brunswick",
        "NL": "Newfoundland and Labrador",
        "NS": "Nova Scotia",
        "NT": "Northwest Territories",
        "NU": "Nunavut",
        "ON": "Ontario",
        "PE": "Prince Edward Island",
        "QC": "Québec",
        "SK": "Saskatchewan",
        "YT": "Yukon",
    }

    # dict of dict additional addresses
    # FSA -> {CPSO number -> address}
    additional_addresses_dict = {}

    # Also map full names to themselves
    _PROVINCES.update({v: v for v in _PROVINCES.values()})

    _prov_pattern = "|".join(re.escape(k) for k in _PROVINCES.keys())
    _addr_re = re.compile(
        rf"^(?P<city>.+?)\s+(?P<prov>{_prov_pattern})\s+(?P<postal>[A-Za-z]\d[A-Za-z]\s*\d[A-Za-z]\d)$"
    )

    def __init__(
        self,
        concurrency: int = 1,
        timeout: httpx.Timeout = None,
    ):
        # check if additional_addresses.json exists
        if not os.path.exists("additional_addresses.json"):
            total = 0
        else:
            data = json.load(
                open("additional_addresses.json")
            )  # read CPSO numbers from a file (dict with FSA as key)
            total = sum(len(cpsonums) for cpsonums in data.values())

        self.console = Console()
        concurrency = max(concurrency, total)
        self.concurrency = concurrency
        self.console.log(f"[yellow] ADDITIONAL ADDR Concurrency: {concurrency}")

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

    async def fetch_one(self, cpso_num: int) -> tuple[int, dict]:
        async with self.sem:
            backoff = 1.0
            for attempt in range(1, 9):
                try:
                    # self.console.log(f"[yellow]Fetching {cpso_num} (attempt {attempt})")
                    resp = await self.client.get(
                        self.BASE_URL, params={"cpsonum": cpso_num}
                    )
                    resp.raise_for_status()
                    soup = BeautifulSoup(resp.text, "lxml")
                    # self.console.log("[green]Fetched data for", cpso_num)
                    return 1, soup

                except httpx.HTTPStatusError as he:
                    self.console.log(
                        f"[yellow]{cpso_num} → HTTP {he.response.status_code}"
                    )
                except httpx.HTTPError as e:
                    self.console.log(f"[yellow]{cpso_num} → Network error: {e}")
                await asyncio.sleep(backoff + random.random() * 0.5)
                backoff = min(backoff * 2, 10.0)
        return 0, {}

    def parse_address(
        self, raw_html: str, raw_phone: str, raw_fax: str
    ) -> Optional[Dict]:
        lines = [ln.strip() for ln in re.split(r"<br\s*/?>", raw_html) if ln.strip()]
        if not lines:
            return None

        last = lines[-1]
        m = self._addr_re.match(last)
        if m:
            city = m.group("city")
            prov = self._PROVINCES[m.group("prov")]
            postal = m.group("postal")
            streets = lines[:-1]
            entry = {
                "street1": streets[0] if streets else "",
                "street2": streets[1] if len(streets) > 1 else "",
                "street3": streets[2] if len(streets) > 2 else "",
                "street4": streets[3] if len(streets) > 3 else "",
                "city": city,
                "province": prov,
                "postalcode": postal,
                "phonenumber": raw_phone,
                "fax": raw_fax,
            }
        else:
            entry = {
                "raw": raw_html,
                "city": None,
                "province": None,
                "postalcode": None,
            }

        return entry

    async def scrape(self, cpsonum: int) -> List[Dict]:
        status, soup = await self.fetch_one(cpsonum)
        if status != 1:
            self.console.log(f"[red]Failed to fetch data for {cpsonum}")
            return []

        practice_info = soup.find("div", class_="list-content scrp-additionalinfo")

        # extract address, phone, and fax
        addresses = []
        phones = []
        faxes = []

        for row in practice_info.find_all("div", class_="scrp-additionalinfo-row"):
            addr = row.find("span", class_="scrp-practiceaddress-value")
            if addr:
                addresses.append(addr.decode_contents())
            else:
                addresses.append("")

            phone = row.find("span", class_="scrp-phone-value")
            if phone:
                phones.append(clean_number(phone.decode_contents()))
            else:
                phones.append("")

            fax = row.find("span", class_="scrp-fax-value")
            if fax:
                faxes.append(clean_number(fax.decode_contents()))
            else:
                faxes.append("")

        additionalAddresses = []
        for addr, phone, fax in zip_longest(addresses, phones, faxes, fillvalue=""):
            if not addr:
                continue
            parsed = self.parse_address(addr, phone, fax)
            if parsed:
                additionalAddresses.append(parsed)

        return additionalAddresses

    async def _worker(self, queue: asyncio.Queue, progress, task_id: int):
        while True:
            cpso_num, FSA = await queue.get()
            FSA = FSA.upper()
            # self.console.log(f"Processing {cpso_num} for {FSA}")
            try:
                # extract array of addresses
                addresses = await self.scrape(cpso_num)
                # add array of additional addresses to the dict
                self.additional_addresses_dict.setdefault(FSA, {})[cpso_num] = addresses
                # print(f"{FSA} {cpso_num} → {len(addresses)} addresses")
                # print(
                #     json.dumps(self.additional_addresses_dict[FSA][cpso_num], indent=2)
                # )
                progress.update(
                    task_id, advance=1, description=f"Done {FSA} {cpso_num}"
                )
            finally:
                queue.task_done()

    async def run(self):
        queue = asyncio.Queue()
        data = json.load(
            open("additional_addresses.json")
        )  # read CPSO numbers from a file (dict with FSA as key)
        total = sum(len(cpsonums) for cpsonums in data.values())

        self.console.log(f"Total CPSO numbers to process: {total}")
        for fsa, lst in data.items():
            for cpso in lst:
                queue.put_nowait((cpso, fsa))

        with Progress(
            SpinnerColumn(),
            TextColumn("{task.description}"),
            BarColumn(),
            TextColumn("{task.completed}/{task.total}"),
            console=self.console,
        ) as prog:
            task_id = prog.add_task("Scraping Additional Addresses", total=total)
            # launch a fixed number of workers
            workers = [
                asyncio.create_task(self._worker(queue, prog, task_id))
                for _ in range(self.concurrency)
            ]
            await queue.join()
            for w in workers:
                w.cancel()

            await self.client.aclose()

            self.update_addresses()
            self.console.log("All tasks completed.")

    def update_addresses(self):
        # print each key in the dict
        for FSA, cpsonums in self.additional_addresses_dict.items():
            FSA = FSA.upper()
            # open extracted_data/{FSA}.json into a data dict
            try:
                with open(f"extracted_data/{FSA}.json", "r") as f:
                    data = demjson3.decode(f.read())
            except FileNotFoundError:
                self.console.log(f"[red]File not found for {FSA}, skipping...")
                continue

            # update the data dict with the additional addresses
            for cpso_num, addresses in cpsonums.items():
                # check if the key exists in the data dict
                if cpso_num not in data:
                    self.console.log(f"[red]CPSO number {cpso_num} not found in {FSA}")
                    continue

                # add the additional addresses to the data dict
                data[cpso_num]["additionalAddresses"] = addresses

            # overwrite the file with the updated data dict
            with open(f"extracted_data/{FSA}.json", "w") as f:
                f.write(json.dumps(data, indent=2))


async def main():
    scraper = PhysicianScraper()
    await scraper.run()
    # data = await scraper.scrape(58310)
    # print(json.dumps(data, indent=2))


if __name__ == "__main__":
    uvloop.run(main())
