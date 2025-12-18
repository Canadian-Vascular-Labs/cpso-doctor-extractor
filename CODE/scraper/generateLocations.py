import time
import requests
from bs4 import BeautifulSoup
import random
time.sleep(random.uniform(0.6, 1.2))

HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/120.0.0.0 Safari/537.36"
    ),
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8",
    "Accept-Language": "en-US,en;q=0.9",
    "Referer": "https://postal-codes.cybo.com/",
    "Connection": "keep-alive",
}


BASE = "https://postal-codes.cybo.com"
ONTARIO_URL = f"{BASE}/canada/ontario/"

session = requests.Session()
session.headers.update(HEADERS)

def get_soup(url, max_retries=3):
    for attempt in range(1, max_retries + 1):
        r = session.get(url, timeout=20)

        # Cybo bot-block redirect
        if "cybo.com/sorry" in r.url:
            wait = min(60, 10 * attempt) + random.uniform(0, 5)
            print(f"⚠️ Cybo block on {url} — retry {attempt}/{max_retries}, sleeping {wait:.1f}s")
            time.sleep(wait)
            continue

        r.raise_for_status()
        return BeautifulSoup(r.text, "html.parser")

    raise RuntimeError(f"Blocked by Cybo after {max_retries} retries: {url}")



def extract_fsa_links():
    fsa_links = {}
    page = 1

    while True:
        url = ONTARIO_URL if page == 1 else f"{ONTARIO_URL}?p={page}"
        soup = get_soup(url)

        table = soup.select_one("#listcodes table tbody")
        if not table:
            break

        rows = table.select("tr")
        if not rows:
            break

        before = len(fsa_links)

        for row in rows:
            a = row.select_one("td a[href]")
            if not a:
                continue

            fsa = a.get_text(strip=True)
            href = a["href"]

            if len(fsa) == 3 and fsa[1].isdigit():
                fsa_links[fsa] = href

        if len(fsa_links) == before:
            break  # no new FSAs found → end

        print(f"[page {page}] total FSAs so far: {len(fsa_links)}")
        page += 1

    return fsa_links


def extract_ldus(fsa, url):
    soup = get_soup(url)
    ldus = []

    # Postal codes appear as links like "K0C 1A0"
    for a in soup.select("a"):
        code = a.get_text(strip=True)
        if code.startswith(fsa) and len(code.replace(" ", "")) == 6:
            ldus.append(code[4:7])  # extract LDU part

    return sorted(set(ldus))

def main():
    fsa_to_ldus = {}

    fsa_links = extract_fsa_links()
    print(f"[+] Found {len(fsa_links)} FSAs in Ontario")

    for i, (fsa, url) in enumerate(sorted(fsa_links.items()), 1):
        if (fsa != "L0L"):
            continue  # 🔴 TESTING ONLY
        print(f"[{i}/{len(fsa_links)}] Scraping {fsa}")
        try:
            ldus = extract_ldus(fsa, url)
            fsa_to_ldus[fsa] = ldus
        except Exception as e:
            print(f"    ⚠️ Failed {fsa}: {e}")
            fsa_to_ldus[fsa] = []

        time.sleep(0.5)  # be polite
    # save results to file
    with open("fsa_to_ldus_L0L.json", "w") as f:
        import json
        json.dump(fsa_to_ldus, f, indent=2)

    return fsa_to_ldus

if __name__ == "__main__":
    data = main()
    print("\nExample:")
    sample = next(iter(data.items()))
    print(sample[0], "→", sample[1][:5])
