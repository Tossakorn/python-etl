import pyodbc
import pandas as pd
import psycopg2


# Connect PostgreSQL
pg_conn = psycopg2.connect(
    host="localhost",        # หรือ IP ของ PostgreSQL server
    port="5432",             # port ปกติคือ 5432
    database="airflow",      # ชื่อ database
    user="postgres",        # username
    password="passport" # password
)

pg_cursor = pg_conn.cursor()

# ทดสอบ query
pg_cursor.execute("SELECT version();")
result = pg_cursor.fetchone()
print("PostgreSQL version:", result)

# ปิด connection
pg_conn.close()
print("Connected successfully!")