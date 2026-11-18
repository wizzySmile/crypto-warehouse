import os
import streamlit as st
import pandas as pd
import psycopg2
from dotenv import load_dotenv
import time

load_dotenv()

# Prefer the explicit Neon setting; retain the legacy key as a fallback.
try:
    NEON_DATABASE_URL = st.secrets["NEON_DATABASE_URL"]
except (KeyError, FileNotFoundError):
    try:
        NEON_DATABASE_URL = st.secrets["DATABASE_URL"]
    except (KeyError, FileNotFoundError):
        NEON_DATABASE_URL = None

NEON_DATABASE_URL = (
    NEON_DATABASE_URL
    or os.getenv("NEON_DATABASE_URL")
    or os.getenv("DATABASE_URL")
)

if not NEON_DATABASE_URL:
    raise RuntimeError("NEON_DATABASE_URL is not configured")

st.set_page_config(page_title="Crypto Warehouse", layout="wide")
st.title("Crypto Warehouse - Neon Dashboard")


@st.cache_data(ttl=60)
def load_data():
    conn = psycopg2.connect(NEON_DATABASE_URL)
    df = pd.read_sql(
        "SELECT * FROM crypto_prices ORDER BY snapshot_date", conn)
    conn.close()
    return df


df = load_data()

if df.empty:
    st.warning("No data yet. Run python crypto_warehouse.py first")
    st.stop()

# Filters
coins = st.multiselect("Select coins", sorted(
    df['coin_id'].unique()), default=sorted(df['coin_id'].unique()))
df_filtered = df[df['coin_id'].isin(coins)]

# KPIs
col1, col2, col3 = st.columns(3)
col1.metric("Total Rows", len(df))
col2.metric("Coins", df['coin_id'].nunique())
col3.metric(
    "Date Range", f"{df['snapshot_date'].min()} to {df['snapshot_date'].max()}")

# Price Chart
st.subheader("Price Over Time (USD)")
pivot = df_filtered.pivot_table(
    index='snapshot_date', columns='coin_id', values='price_usd')
st.line_chart(pivot)

# Market Cap Chart
st.subheader("Market Cap Over Time")
pivot_cap = df_filtered.pivot_table(
    index='snapshot_date', columns='coin_id', values='market_cap_usd')
st.line_chart(pivot_cap)

# Table
st.subheader("Raw Data")
st.dataframe(df_filtered.sort_values('snapshot_date',
             ascending=False), use_container_width=True)

time.sleep(300)
st.rerun()
