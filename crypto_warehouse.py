from botocore.config import Config
import pandas as pd
import duckdb as db
import requests
import logging
import json
import time
import boto3
import os
from dotenv import load_dotenv
from datetime import datetime, timezone
from pathlib import Path

PROJECT_ROOT = os.path.dirname(os.path.abspath(__file__))
DB_PATH = os.path.join(PROJECT_ROOT, "crypto_warehouse.duckdb")


# Load Cloudflare R2 credentials from local .env file
# In Fly.io these values are provided through secrets instead
load_dotenv()
R2_ACCOUNT_ID = os.getenv("R2_ACCOUNT_ID")
R2_ACCESS_KEY = os.getenv("R2_ACCESS_KEY")
R2_SECRET_KEY = os.getenv("R2_SECRET_KEY")
R2_ENDPOINT = os.getenv("R2_ENDPOINT")
R2_BUCKET = os.getenv("R2_BUCKET")


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
    
    # Retry up to 3 times to handle temporary API failures
    # such as timeouts and rate limits
    for attempt in range(3):

        logging.info(f"{coin} - Attempt {attempt + 1}")
        # error handling.. 
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
            # Exponential backoff:
            # 5s → 10s → 20s
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

    # Ensure all requested coins were successfully extracted
    expected_rows = len(coins)
    if df.empty:
        logging.error("DataFrame is empty")
        return False

    # Detect duplicate business keys before loading
    # (coin_id + snapshot_date)
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

    conn = db.connect(DB_PATH)

    conn.register("crypto_df", df)

    # Historical warehouse table storing daily crypto snapshots
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

    # Business key:
    # A crypto snapshot is uniquely identified by
    # coin_id + snapshot_date.
    # Prevent duplicate loads when the pipeline is re-run.
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


def upload_to_r2(file_path):

    # Try new config, fall back to old if needed (for old containers)
    try:
        from botocore import __version__ as botocore_version
        print(f"botocore version: {botocore_version}")
    except:
        pass

    try:
        s3_client = boto3.client(
            's3',
            endpoint_url=R2_ENDPOINT,
            aws_access_key_id=R2_ACCESS_KEY,
            aws_secret_access_key=R2_SECRET_KEY,
            region_name='auto',
            config=Config(
                request_checksum_calculation='when_required',
                response_checksum_validation='when_required',
            )
        )
    except TypeError:
        # Old botocore in Docker - use simple config
        print("Using fallback config for old botocore")
        s3_client = boto3.client(
            's3',
            endpoint_url=R2_ENDPOINT,
            aws_access_key_id=R2_ACCESS_KEY,
            aws_secret_access_key=R2_SECRET_KEY,
            region_name='auto',
            config=Config(signature_version='s3v4')
        )

    now = datetime.now(timezone.utc)
    # Store files using date-based partitions to create
    # a scalable data lake structure
    snapshot_date = datetime.now().date()
    r2_key = f"crypto/{now.year}/{now.month:02d}/{now.day:02d}/crypto_prices_{snapshot_date}.parquet"

    logging.info(f"Uploading to R2: {r2_key}")
    s3_client.upload_file(str(file_path), R2_BUCKET, r2_key)
    logging.info(f"Upload successful: {r2_key}")
    return r2_key


# ---------------------------
# Main Pipeline
# ---------------------------

def run_pipeline():

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
    # parquet_file = (f"crypto_prices_{snapshot_date}.parquet")
    parquet_file = os.path.join(
        PROJECT_ROOT, f"crypto_prices_{snapshot_date}.parquet")
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

    end_time = time.time()

    # Capture total execution time for monitoring
    # and performance tracking
    duration = round(end_time - start_time, 2)
    logging.info(f"pipeline duration: {duration} seconds")
    logging.info("pipeline completed")


# run pipeline
if __name__ == "__main__":
    run_pipeline()
