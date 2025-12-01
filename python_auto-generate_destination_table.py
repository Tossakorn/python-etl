# Python auto-generate

# === Import ===
import os
import warnings
import pandas as pd
from dotenv import load_dotenv
from sqlalchemy import create_engine
import urllib.parse

# Ignore pandas warning
warnings.filterwarnings('ignore', category=UserWarning)

# Load .env
load_dotenv()

# === MSSQL Engine ===
mssql_params = urllib.parse.quote_plus(
    f"DRIVER={{ODBC Driver 17 for SQL Server}};"
    f"SERVER={os.getenv('DEV_MSSQL_SERVER')};"  # ไม่ต้องมี port ถ้าใช้ default
    f"DATABASE={os.getenv('DEV_MSSQL_DATABASE')};"
    f"UID={os.getenv('DEV_MSSQL_USER')};"
    f"PWD={os.getenv('DEV_MSSQL_PASSWORD')};"
)
dev_mssql_engine = create_engine(f"mssql+pyodbc:///?odbc_connect={mssql_params}")
print("1. Connected to MSSQL")

# === PostgreSQL Engine ===
dev_pg_engine = create_engine(
    f"postgresql://{os.getenv('DEV_PG_USER')}:{os.getenv('DEV_PG_PASSWORD')}"
    f"@{os.getenv('DEV_PG_HOST')}:{os.getenv('DEV_PG_PORT')}/{os.getenv('DEV_PG_DATABASE')}"
)
print("2. Connected to PostgreSQL")

# === Extract ===

# sql = "SELECT top 20020 * FROM pos.pp_activity"  # ← แก้เป็นชื่อ table จริง
# df = pd.read_sql(sql, dev_mssql_engine)
# print(f"3. Extracted: {len(df)} rows")

sql = "SELECT  top 80020 * FROM pos.pp_activity with(nolock)"
# === Extract + Load แบบ chunk ===
total_rows = 0
for chunk_df in pd.read_sql(sql, dev_mssql_engine, chunksize=10000):
    chunk_df.to_sql(
        'snap_pp_activity',
        dev_pg_engine,
        schema='dwh',
        if_exists='append',
        index=False,
        method='multi'
    )
    total_rows += len(chunk_df)
    print(f"Inserted {len(chunk_df)} rows (Total: {total_rows})")

print(f"4. Insert completed! Total: {total_rows} rows")

# === Verify ===
result = pd.read_sql("SELECT COUNT(*) as cnt FROM dwh.snap_pp_activity", dev_pg_engine)
print(f"5. Verified: {result['cnt'][0]} rows in destination")

# === Close ===
dev_mssql_engine.dispose()
dev_pg_engine.dispose()
print("6. Connections closed")