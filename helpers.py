import re
import json
from itertools import zip_longest
from typing import List, Dict, Optional

import requests
from bs4 import BeautifulSoup


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

    # Also map full names to themselves
    _PROVINCES.update({v: v for v in _PROVINCES.values()})

    _prov_pattern = "|".join(re.escape(k) for k in _PROVINCES.keys())
    _addr_re = re.compile(
        rf"^(?P<city>.+?)\s+(?P<prov>{_prov_pattern})\s+(?P<postal>[A-Za-z]\d[A-Za-z]\s*\d[A-Za-z]\d)$"
    )

    def __init__(self, user_agent: Optional[str] = None, timeout: float = 10.0):
        # Configure a session with browser-like headers
        self.session = requests.Session()
        ua = user_agent or (
            "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
            "AppleWebKit/537.36 (KHTML, like Gecko) "
            "Chrome/114.0.0.0 Safari/537.36"
        )
        self.session.headers.update(
            {
                "User-Agent": ua,
                "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
                "Accept-Language": "en-US,en;q=0.5",
                "Referer": "https://register.cpso.on.ca/",
            }
        )
        self.timeout = timeout

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

    def scrape(self, cpsonum: int) -> List[Dict]:
        url = f"https://register.cpso.on.ca/physician-info/?cpsonum={cpsonum}"
        resp = self.session.get(url, timeout=self.timeout)
        resp.raise_for_status()
        soup = BeautifulSoup(resp.text, "html.parser")

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


if __name__ == "__main__":
    scraper = PhysicianScraper()
    data = scraper.scrape(58310)
    print(json.dumps(data, indent=2))
