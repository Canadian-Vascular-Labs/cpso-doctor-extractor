# main.py
import asyncio
from scraper import CPSOScraper
from db import DBStreamClient

async def main():
    start_time = asyncio.get_event_loop().time()
    stop_event = asyncio.Event()

    db = DBStreamClient(
        host="localhost",
        port=3307,
        user="root",
        password="secret",
        db="mydb",
        table="doctors",
        batch_size=200,
        flush_interval=3.0,
    )
    await db.connect()

    scraper = CPSOScraper(
        concurrency=50,
        base_url="https://register.cpso.on.ca/Get-Search-Results/",
        db=db,  # 👈 inject dependency
    )

    try:
        await scraper.run(stop_event)
    finally:
        await db.close()
    end_time = asyncio.get_event_loop().time()
    print(f"Total time: {end_time - start_time:.2f} seconds")

if __name__ == "__main__":
    asyncio.run(main())
