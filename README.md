# Crypto Warehouse Pipeline

Automated pipeline that fetches top 100 coins from CoinGecko API, stores in Parquet and DuckDB warehouse with logging and deduplication.

## Stack
- Python, Pandas, DuckDB, Parquet
- Airflow-ready DAG
- Logging & error handling

## Run
python crypto_warehouse.py