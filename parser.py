import os
import pandas as pd
from datetime import datetime
from database import SessionLocal
from models import SpimexTradingResult, Base
from sqlalchemy.exc import SQLAlchemyError
import aiohttp
from aiohttp.client_exceptions import ClientResponseError

BASE_URL = "https://spimex.com/upload/reports/oil_xls/"
DOWNLOAD_DIR = "downloads"

async def init_schema():
    Base.metadata.create_all(bind=SessionLocal().bind)

async def download_excel_file(session: aiohttp.ClientSession, file_name: str) -> str | None:
    os.makedirs(DOWNLOAD_DIR, exist_ok=True)
    path = os.path.join(DOWNLOAD_DIR, file_name)

    if os.path.exists(path):
        return path

    url = f"{BASE_URL}{file_name}?r=1404"
    try:
        async with session.get(url) as resp:
            if resp.status == 200:
                with open(path, "wb") as f:
                    f.write(await resp.read())
                return path
            elif resp.status == 404:
                print('Error 404 File not found')
            else:
                print(f"[DOWNLOAD ERROR] {file_name}: status {resp.status}")
                return None
    except ClientResponseError as e:
        print(f"[DOWNLOAD ERROR] {file_name}: {e}")
        return None

def clean_col_name(col):
    parts = [str(p).strip() for p in col if 'Unnamed' not in str(p)]
    return ' '.join(parts).replace('\n', ' ')

async def parse_and_store(file_path: str, trade_date: datetime):
    try:
        df_all = pd.read_excel(file_path, sheet_name=None, header=[6, 7], index_col=0)
    except Exception as e:
        print(f"[PARSE ERROR] {file_path}: {e}")
        return

    total_added = 0
    for sheet_name, df in df_all.items():
        df.columns = [clean_col_name(col) for col in df.columns]
        df.columns = df.columns.str.replace('\n', ' ').str.strip()
        df.columns = df.columns.str.replace('Обьем Договоров, руб.', 'Объем Договоров, руб.')

        required = [
            'Код Инструмента',
            'Наименование Инструмента',
            'Базис поставки',
            'Объем Договоров в единицах измерения',
            'Объем Договоров, руб.',
            'Количество Договоров, шт.'
        ]

        if not all(col in df.columns for col in required):
            print(f"[SKIP SHEET] {sheet_name}: не все требуемые колонки найдены")
            continue

        df_filtered = df[required]
        df_filtered = df_filtered[pd.to_numeric(df_filtered['Количество Договоров, шт.'], errors='coerce').fillna(0) > 0]

        df_filtered['Объем Договоров в единицах измерения'] = pd.to_numeric(df_filtered['Объем Договоров в единицах измерения'], errors='coerce').fillna(0)
        df_filtered['Объем Договоров, руб.'] = pd.to_numeric(df_filtered['Объем Договоров, руб.'], errors='coerce').fillna(0)
        df_filtered['Количество Договоров, шт.'] = pd.to_numeric(df_filtered['Количество Договоров, шт.'], errors='coerce').fillna(0).astype(int)

        session = SessionLocal()
        count = 0

        try:
            for _, row in df_filtered.iterrows():
                eid = str(row['Код Инструмента'])
                if eid.startswith('Итого'):
                    continue

                obj = SpimexTradingResult(
                    exchange_product_id=eid,
                    exchange_product_name=row['Наименование Инструмента'],
                    oil_id=eid[:4],
                    delivery_basis_id=eid[4:7],
                    delivery_basis_name=row['Базис поставки'],
                    delivery_type_id=eid[-1],
                    volume=row['Объем Договоров в единицах измерения'],
                    total=row['Объем Договоров, руб.'],
                    count=row['Количество Договоров, шт.'],
                    date=trade_date.date()
                )
                session.add(obj)
                count += 1

            session.commit()
        except SQLAlchemyError as db_err:
            session.rollback()
            print(f"[DB ERROR] {trade_date.date()}: {db_err}")
        finally:
            session.close()

        total_added += count

    print(f"[DB] Добавлено записей для даты {trade_date.date()}: {total_added}")
