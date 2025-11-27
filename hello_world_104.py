import os
import pyodbc
import psycopg2
from dotenv import load_dotenv

load_dotenv()

# ========== 1. Connect MSSQL (Source) ==========
mssql_conn = pyodbc.connect(
    f"Driver={{ODBC Driver 17 for SQL Server}};"
    f"Server={os.getenv('DEV_MSSQL_SERVER')};"
    f"Database={os.getenv('DEV_MSSQL_DATABASE')};"
    f"UID={os.getenv('DEV_MSSQL_USER')};"
    f"PWD={os.getenv('DEV_MSSQL_PASSWORD')};"
)
mssql_cursor = mssql_conn.cursor()
print("Connected to MSSQL")

# ========== 2. Connect PostgreSQL (Target) ==========
pg_conn = psycopg2.connect(
    host=os.getenv("DEV_PG_HOST"),
    port=os.getenv("DEV_PG_PORT"),
    database=os.getenv("DEV_PG_DATABASE"),
    user=os.getenv("DEV_PG_USER"),
    password=os.getenv("DEV_PG_PASSWORD")
)
pg_cursor = pg_conn.cursor()
print("Connected to PostgreSQL")

# ========== 3. ดึงข้อมูลจาก MSSQL ==========
mssql_cursor.execute("SELECT TOP 100000 book_id, book_no FROM [Dev_da].[STG].[stg_nbk_booking]")
rows = mssql_cursor.fetchall()
print(f"ดึงข้อมูลได้ {len(rows)} rows")

# ========== 4. Insert ไป PostgreSQL ==========
for row in rows:
    pg_cursor.execute(
        "INSERT INTO stg_nbk (book_id, book_no) VALUES (%s, %s)",
        (row[0], row[1])
    )

pg_conn.commit()
print("Insert สำเร็จ!")

# ========== 5. ปิด connection ==========
mssql_conn.close()
pg_conn.close()
print("ETL completed!")