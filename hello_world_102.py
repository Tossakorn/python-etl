import pyodbc
import pandas as pd
#import psycopg2

server = '192.168.70.129'
database = 'Dev_DA'
username = 'dev_tossakorn'
password = 'n0bl3@2023'

conn_str = (
    "DRIVER={ODBC Driver 17 for SQL Server};"
    f"SERVER={server};"
    f"DATABASE={database};"
    f"UID={username};"
    f"PWD={password}"
)



conn = pyodbc.connect(conn_str)
cursor = conn.cursor()

cursor.execute("SELECT top 10 book_id, book_no FROM [Dev_DA].[STG].[stg_nbk_booking]")
rows = cursor.fetchall()

# path ที่จะใช้ lib ของ pandas
# สร้าง DataFrame
columns = [column[0] for column in cursor.description]  # ดึงชื่อ column
df = pd.DataFrame.from_records(rows, columns=columns)

print(df)


