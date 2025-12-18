# fetch.py
import asyncio, random, json, re
import httpx
import demjson3

def repair_broken_json(raw: str) -> str:
    raw = re.sub(r"[\x00-\x1F\x7F]", "", raw)
    raw = raw.replace('\\"', '"')
    raw = re.sub(r'(?<!\\)\\(?![\\nt"r/])', r"\\\\", raw)
    raw = re.sub(r",\s*,+", ",", raw)
    raw = re.sub(r",\s*]", "]", raw)
    return raw


async def fetch_one(client, sem, url, params, label):
    backoff = 1.0
    async with sem:
        for attempt in range(1, 9):
            try:
                print(f"[fetch_one] {label} Attempt {attempt} - {url} {params}")
                resp = await client.get(url, params=params)
                resp.raise_for_status()
                try:
                    return json.loads(resp.text)
                except json.JSONDecodeError:
                    return demjson3.decode(repair_broken_json(resp.text))
            except httpx.HTTPStatusError as e:
                if e.response.status_code in {429, 403}:
                    await asyncio.sleep(backoff + random.random())
                    backoff = min(backoff * 2, 60)
            except httpx.HTTPError:
                await asyncio.sleep(backoff)
                backoff = min(backoff * 2, 60)
    return None
