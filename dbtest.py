import duckdb
import os

conn = duckdb.connect()
conn.execute("INSTALL delta; LOAD delta;")

# Export each gold table to SQLite
tables = {
    "orders_base": "lakehouse/gold/orders_base",
    "shipping_performance": "lakehouse/gold/shipping_performance",
    "sales_performance": "lakehouse/gold/sales_performance",
    "fulfillment": "lakehouse/gold/fulfillment",
}

for table_name, path in tables.items():
    conn.execute(f"""
        CREATE OR REPLACE TABLE {table_name} AS 
        SELECT * FROM delta_scan('{path}')
    """)

conn.execute("ATTACH 'lakehouse/superset.db' AS sqlite_db (TYPE SQLITE)")

for table_name in tables.keys():
    conn.execute(f"CREATE OR REPLACE TABLE sqlite_db.{table_name} AS SELECT * FROM {table_name}")

print("Done — lakehouse/superset.db ready")
conn.close()