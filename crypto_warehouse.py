# ✅ Requests
# ✅ Error Handling
# ✅ Logging
# ✅ JSON
# ✅ Sessions
# ✅ Functions
# ✅ Extract
# ✅ Transform
# ✅ Load(DuckDB)

# ✅ Validate
# ✅ Parquet
# ✅ DuckDB querying Parquet
# ✅ Scheduling
# ➡ Data Warehouse Modelling
# ➡ Cloud Storage ➡ Cloudflare R2
# ➡ Airflow
# ➡ PySpark


import pandas as pd
import duckdb as db
import requests
import logging
import schedule
import json
import time
import boto3
import os
from dotenv import load_dotenv
from datetime import datetime


load_dotenv() # reading secret file

# Create s3 client INSIDE the function - this fixes your error
s3 = boto3.client(
    "s3",
    endpoint_url=os.getenv("R2_ENDPOINT"),
    aws_access_key_id=os.getenv("R2_ACCESS_KEY"),
    aws_secret_access_key=os.getenv("R2_SECRET_KEY"),
    region_name="auto"
)
# ---------------------------
# Logging Configuration
# ---------------------------

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
    handlers=[
        logging.FileHandler(
            "crypto_logging.log",
            mode="w"
        ),
        logging.StreamHandler()
    ]
)

# ---------------------------
# Parameters
# ---------------------------
coins = [
    "bitcoin",
    "ethereum",
    "tether",
    "binancecoin",
    "ripple"
]

# ---------------------------
# Extract
# ---------------------------

def extract_coin_data(
    coin,
    snapshot_date,
    session
):

    url = (
        f"https://api.coingecko.com/api/v3/coins/{coin}/history?date={snapshot_date}")

    for attempt in range(3):

        logging.info(f"{coin} - Attempt {attempt + 1}")

        try:

            logging.info(f"Fetching {coin} data")

            response = session.get(
                url,
                timeout=10
            )

            response.raise_for_status()

            data = response.json()
            logging.info(f"{coin} data pull completed")

            return data

        except requests.exceptions.Timeout:

            wait_time = 5 * (2 ** attempt)

            logging.warning(f"{coin} timed out. Waiting {wait_time} seconds before retry...")
            time.sleep(wait_time)
            continue

        except requests.exceptions.HTTPError as e:

            wait_time = 5 * (2 ** attempt)

            if response.status_code == 429:

                logging.warning(f"{coin} rate limited. Waiting {wait_time} seconds before retry...")
                time.sleep(wait_time)
                continue

            logging.error(f"{coin} HTTP Error: {e}")
            return None

        except json.JSONDecodeError as e:
            logging.error(f"{coin} returned bad JSON: {e}")
            return None

        except requests.exceptions.RequestException as e:
            logging.error(f"{coin} Request Error: {e}")
            return None

        finally:
            logging.info(f"{coin} attempt {attempt + 1} completed")

    return None


# ---------------------------
# Transform
# ---------------------------

def transform_data(
    data,
    snapshot_date
):

    record = {
        "coin_id": data["id"],
        "symbol": data["symbol"],
        "snapshot_date": snapshot_date,
        "price_usd": data["market_data"]["current_price"]["usd"],
        "market_cap_usd": data["market_data"]["market_cap"]["usd"],
        "total_volume_usd": data["market_data"]["total_volume"]["usd"],
        "ingestion_time": datetime.now()
    }
    return record


# ---------------------------
# Validate
# ---------------------------

def validate_data(df):
    expected_rows = len(coins)
    if df.empty:
        logging.error("DataFrame is empty")
        return False
    
    duplicate_count = df.duplicated(
        subset=[
            "coin_id",
            "snapshot_date"]).sum()
    
    if len(df) != expected_rows:
        logging.error(f"Expected {expected_rows} rows but got {len(df)}")
        return False

    if duplicate_count > 0:
        logging.error(f"Found {duplicate_count} duplicates")
        return False
    
    logging.info(f"Rows extracted: {len(df)}")
    return True


# ---------------------------
# Load
# ---------------------------

def load_to_duckdb(df):

    logging.info("Loading data into DuckDB")

    conn = db.connect("crypto_warehouse.duckdb")

    conn.register("crypto_df", df)

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

    before_count = conn.execute("""
    SELECT COUNT(*)
    FROM crypto_prices
    """).fetchone()[0]

    conn.execute("""
        INSERT INTO crypto_prices
        SELECT *
        FROM crypto_df cd
        WHERE NOT EXISTS (
        SELECT 1
        FROM crypto_prices cp

        WHERE cp.coin_id = cd.coin_id
        AND cp.snapshot_date = cd.snapshot_date

)
    """)
    after_count = conn.execute("""
    SELECT COUNT(*)
    FROM crypto_prices
    """).fetchone()[0]

    conn.close()

    rows_inserted = after_count - before_count
    logging.info(f"Rows inserted: {rows_inserted}")
    logging.info("DuckDB load completed")


def upload_to_r2(parquet_file):

    s3.upload_file(
        parquet_file,
        os.getenv("R2_BUCKET"),
        parquet_file
    )

    logging.info(f"{parquet_file} uploaded to R2")


# ---------------------------
# Main Pipeline
# ---------------------------

def run_pipeline():

    # starting pipeline
    start_time = time.time()
    logging.info("pipeline started..")


# ---------------------------
# Session
# ---------------------------
    session = requests.Session()

    session.headers.update({
        "accept": "application/json"
    })
    snapshot_date = datetime.now().date()


    crypto_data = []

    for coin in coins:
        

        raw_data = extract_coin_data(
            coin,
            snapshot_date,
            session
        )

        if raw_data:

            clean_data = transform_data(
                raw_data,
                snapshot_date
            )

            crypto_data.append(clean_data)


    crypto_df = pd.DataFrame(crypto_data)

    logging.info("Crypto data cleaning completed")


    # ---------------------------
    # Save Parquet
    # ---------------------------
    parquet_file = (f"crypto_prices_{snapshot_date}.parquet")
    
    crypto_df.to_parquet(
        parquet_file,
        index=False
    )
    logging.info("Parquet file created")

    logging.info("uploading `crypto_prices.parquet` to cloudflare")
    upload_to_r2(parquet_file)
    logging.info(f"{parquet_file} uploaded successfully to cloudflare")

    # ---------------------------
    # Validate & Load
    # ---------------------------

    if validate_data(crypto_df):

        load_to_duckdb(crypto_df)

        logging.info("Loaded to data warehouse")

    # pipeline ends.
    end_time = time.time()

    # calculate pipeline duration
    duration = round(end_time - start_time, 2)
    logging.info(f"pipeline duration: {duration} seconds")
    logging.info("pipeline completed")


# run pipeline
if __name__ == "__main__":
    run_pipeline()

# ---------------------------
# Verify Warehouse
# ---------------------------


import duckdb as db
conn = db.connect('crypto_warehouse.duckdb')

result = conn.execute("""
    SELECT *
    FROM crypto_prices
""").df()

print(result)

conn.close()


