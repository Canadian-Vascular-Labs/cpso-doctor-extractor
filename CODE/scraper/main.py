# main.py
import asyncio
from config import BASE_URL
from scraper import CPSOScraper, RunMode
from db import DBStreamClient

# load json data into python object
import json


def load_json_file(filepath):
    with open(filepath, "r") as file:
        data = json.load(file)
    return data


async def main():
    start_time = asyncio.get_event_loop().time()
    db = DBStreamClient(
        host="localhost",
        port=3307,
        user="cvluser",
        password="cvlpass",
        db="cvldb",
        batch_size=200,
        flush_interval=3.0,
    )

    await db.connect()
    await db.preload_cpsos()  # 🔴 IMPORTANT
    num_docs_before = len(db.existing_cpsos)
    print(f"Preloaded {num_docs_before} CPSO records from DB.")
    data = load_json_file("fsa_to_ldus.json")

    scraper = CPSOScraper(
        concurrency=20,
        base_url=BASE_URL,
        db=db,
        run_mode=RunMode.FULL,
        target_cpsos=["58310"],
        ldu_map=data,
    )

    try:
        await scraper.run()
    finally:
        await db.close()

    end_time = asyncio.get_event_loop().time()
    print(f"Total time: {end_time - start_time:.2f} seconds")
    print(f"Total new CPSO records added: {len(db.existing_cpsos) - num_docs_before}")


if __name__ == "__main__":
    asyncio.run(main())
