# Crypto Warehouse
A cloud-native data engineering pipeline that collects cryptocurrency market data, validates and stores historical snapshots in a cloud data lake, and maintains an analytical warehouse for querying and reporting.

## Overview
This project was built to demonstrate the end-to-end lifecycle of a modern data pipeline:

Extracting data from external APIs
Performing validation and quality checks
Building historical datasets
Storing data in a cloud data lake
Loading data into an analytical warehouse
Containerising and deploying workloads to the cloud

The pipeline collects daily cryptocurrency market metrics from the CoinGecko API and stores them as historical snapshots for future analysis.

## Architecture
<img width="1280" height="853" alt="WhatsApp Image 2026-11-02 at 2 58 24 PM" src="https://github.com/user-attachments/assets/c0a6684b-3957-4f3f-a0dc-ce8c5bbc6e84" />

## Deployment:
GitHub ➡️ Docker ➡️ Fly.io

## Key Features
Reliable Data Extraction
Session-based API requests
Timeout handling
Exponential backoff retry strategy
Rate-limit handling (HTTP 429)

## Data Quality Validation
Empty dataset detection
Expected row-count validation
Duplicate detection
Load prevention on validation failure

## Historical Data Storage
Daily snapshots are stored in Cloudflare R2 using date-based partitioning:

```bash
crypto/
└── 2026/
    └── 10/
        ├── 25/
        │   └── crypto_prices_2026-10-25.parquet
        └── 26/
            └── crypto_prices_2026-10-26.parquet
```

## Idempotent Loading
Warehouse loads are protected against duplicate inserts using:
coin_id + snapshot_date

This allows the pipeline to be safely re-run without creating duplicate records.

## Cloud Deployment
The entire solution is:

Dockerised
Managed through GitHub
Deployed on Fly.io
Connected securely to Cloudflare R2 via environment secrets

## Technology Stack
| Layer | Technology |
| --- | --- |
| Language | Python |
| API | CoinGecko |
| Data Processing | Pandas |
| Object Storage | Cloudflare R2 |
| File Format | Parquet |
| Data Warehouse | DuckDB |
| Containerisation | Docker |
| Cloud Platform | Fly.io |
| Version Control | Git & GitHub |
| Orchestration | Apache Airflow |

## Table Schema: `crypto_prices`

| Column | Type |
| --- | --- |
| coin_id | VARCHAR |
| symbol | VARCHAR |
| snapshot_date | DATE |
| price_usd | DOUBLE |
| market_cap_usd | DOUBLE |
| total_volume_usd | DOUBLE |
| ingestion_time | TIMESTAMP |

## Engineering Decisions

### Why Cloudflare R2?
- S3-compatible object storage
- Zero egress fees
- Simple integration with boto3
- Suitable for analytics-focused workloads

### Why Parquet?
Parquet provides:
- Efficient compression
- Columnar storage
- Fast analytical access
- Reduced storage consumption

### Why DuckDB?
DuckDB enables analytical SQL workloads without requiring database infrastructure, making it ideal for local analytics and portfolio projects.

### Why Historical Snapshots?
Historical datasets make it possible to analyse:
- Daily market movement
- Price trends
- Volume trends
- Market capitalisation growth

## Challenges Solved
During development, I implemented solutions for:
- API rate limiting
- Cloud credential management
- Secret handling in Fly.io
- Historical duplicate prevention
- Cloud storage integration
- Container deployment troubleshooting
- Data validation and observability

## Future Improvements
- Build a Star Schema (dim_coin, fact_crypto_prices)
- Query R2 Parquet files directly with DuckDB
- Add Apache Airflow orchestration
- Add automated data quality tests
- Implement alerting and failure notifications
- Introduce PySpark for large-scale processing

## Skills Demonstrated
This project demonstrates practical experience with:
- Data Engineering
- API Integration
- ETL Development
- Data Validation
- Data Warehousing
- Data Lakes
- Cloud Storage
- Docker
- Cloud Deployment
- Secret Management
- Logging & Monitoring
- Git & GitHub

## Author
**Wisdom Mpamugo Chinemere**  
Aspiring Data Engineer focused on building production-style data pipelines, cloud data platforms, and analytics solutions.