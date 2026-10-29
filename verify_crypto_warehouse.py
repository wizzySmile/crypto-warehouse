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
