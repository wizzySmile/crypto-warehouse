from botocore.config import Config
import boto3
from dotenv import load_dotenv
import pandas as pd
import duckdb as db
import requests
import time
import os
import sys
from datetime import date, datetime, timedelta

PROJECT_ROOT = os.path.dirname(os.path.abspath(__file__))
DB_PATH = os.path.join(PROJECT_ROOT, "crypto_warehouse.duckdb")

coins = ["bitcoin", "ethereum", "tether", "binancecoin", "ripple"]


if len(sys.argv) > 1:
    SNAPSHOT_DATE = date.fromisoformat(sys.argv[1])
else:
    SNAPSHOT_DATE = date.today() - timedelta(days=1)

API_DATE = SNAPSHOT_DATE.strftime("%d-%m-%Y")
print(f"Backfilling for: {SNAPSHOT_DATE} (CoinGecko format: {API_DATE})")


def extract_coin_data(coin, session):
    url = f"https://api.coingecko.com/api/v3/coins/{coin}/history?date={API_DATE}"
    for attempt in range(3):
        try:
            print(f"Fetching {coin} for {API_DATE} - Attempt {attempt+1}")
            resp = session.get(url, timeout=10)
            resp.raise_for_status()
            return resp.json()
        except Exception as e:
            print(f"{coin} failed: {e}, retrying...")
            time.sleep(5 * (2 ** attempt))
    return None


def transform_data(data):
    return {
        "coin_id": data["id"],
        "symbol": data["symbol"],
        "snapshot_date": SNAPSHOT_DATE,
        "price_usd": data["market_data"]["current_price"]["usd"],
        "market_cap_usd": data["market_data"]["market_cap"]["usd"],
        "total_volume_usd": data["market_data"]["total_volume"]["usd"],
        "ingestion_time": datetime.now()
    }


session = requests.Session()
session.headers.update({"accept": "application/json"})

crypto_data = []
for coin in coins:
    raw = extract_coin_data(coin, session)
    if raw and "market_data" in raw:
        crypto_data.append(transform_data(raw))
    time.sleep(2)

df = pd.DataFrame(crypto_data)
print(df)

DATA_DIR = os.path.join(PROJECT_ROOT, "data")
os.makedirs(DATA_DIR, exist_ok=True)
parquet_file = os.path.join(DATA_DIR, f"crypto_prices_{SNAPSHOT_DATE}.parquet")
df.to_parquet(parquet_file, index=False)
print(f"Saved: {parquet_file}")

# --- DuckDB load ---
conn = db.connect(DB_PATH)
conn.execute("""
    CREATE TABLE IF NOT EXISTS crypto_prices (
        "coin_id" VARCHAR,
        "symbol" VARCHAR,
        "snapshot_date" DATE,
        "price_usd" DOUBLE,
        "market_cap_usd" DOUBLE,
        "total_volume_usd" DOUBLE,
        "ingestion_time" TIMESTAMP
    );
""")

before = conn.execute("SELECT COUNT(*) FROM crypto_prices").fetchall()[0][0]

conn.execute("""
    INSERT INTO crypto_prices
    SELECT * FROM read_parquet(?)
    WHERE NOT EXISTS (
        SELECT 1 FROM crypto_prices cp
        WHERE cp.coin_id = read_parquet.coin_id
        AND cp.snapshot_date = read_parquet.snapshot_date
    )
""", [parquet_file])

after = conn.execute("SELECT COUNT(*) FROM crypto_prices").fetchall()[0][0]
print(f"Rows inserted: {after - before}")
print(conn.execute("SELECT snapshot_date, COUNT(*) FROM crypto_prices WHERE snapshot_date = ? GROUP BY 1",
      [SNAPSHOT_DATE]).fetchall())
conn.close()

# --- R2 upload - dynamic partition ---

load_dotenv()
s3_client = boto3.client(
    's3',
    endpoint_url=os.getenv("R2_ENDPOINT"),
    aws_access_key_id=os.getenv("R2_ACCESS_KEY"),
    aws_secret_access_key=os.getenv("R2_SECRET_KEY"),
    region_name='auto',
    config=Config(
        request_checksum_calculation='when_required',
        response_checksum_validation='when_required',
    )
)

r2_key = f"crypto/{SNAPSHOT_DATE.year}/{SNAPSHOT_DATE.month:02d}/{SNAPSHOT_DATE.day:02d}/crypto_prices_{SNAPSHOT_DATE}.parquet"
print(f"Uploading to R2: {r2_key}")
s3_client.upload_file(parquet_file, os.getenv("R2_BUCKET"), r2_key)
print(f"Upload successful: {r2_key}")
print("Backfill done!")
