import os
from dotenv import load_dotenv

load_dotenv()

# DEV
print("=== DEV ===")
print("MSSQL Server:", os.getenv("DEV_MSSQL_SERVER"))
print("PG Host:", os.getenv("DEV_PG_HOST"))

# PROD
print("=== PROD ===")
print("MSSQL Server:", os.getenv("PROD_MSSQL_SERVER"))
print("PG Host:", os.getenv("PROD_PG_HOST"))