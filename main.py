import asyncio
import time
from datetime import datetime
import os
import aiohttp

from parser import init_schema, download_excel_file, parse_and_store

semaphore = asyncio.Semaphore(20)

def timing_decorator(corofn):
    async def wrapper(*args, **kwargs):
        start = time.perf_counter()
        result = await corofn(*args, **kwargs)
        end = time.perf_counter()
        print(f"⏱ Completed in {end - start:.2f} seconds")
        print('Sync version Completed in 1208.81 seconds')
        return result
    return wrapper

async def process_date(date: datetime, session: aiohttp.ClientSession):
    file_name = f"oil_xls_{date.strftime('%Y%m%d')}162000.xls"
    async with semaphore:
        try:
            path = await download_excel_file(session, file_name)
            if not path:
                print(f"[✗] {date.date()} skipped: download failed")
                return
            if os.path.getsize(path) < 1000:
                print(f"[✗] {date.date()} — файл слишком маленький, пропускаем")
                return

            await parse_and_store(path, date)
            print(f"[✓] {date.date()} processed")
        except Exception as e:
            print(f"[✗] {date.date()} skipped: {e}")

@timing_decorator
async def main():
    await init_schema()
    async with aiohttp.ClientSession() as session:
        tasks = []
        for year in [2023, 2024, 2025]:
            for month in range(1, 13):
                for day in range(1, 32):
                    try:
                        if year == 2025 and month>7:
                            break
                        date = datetime(year, month, day)
                        tasks.append(asyncio.create_task(process_date(date, session)))
                    except:
                        continue
        await asyncio.gather(*tasks)

if __name__ == "__main__":
    asyncio.run(main())
