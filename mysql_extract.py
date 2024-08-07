import os
import duckdb

MYSQL_USERNAME = "root"
MYSQL_PASSWORD = ""
MYSQL_HOST = "localhost"
MYSQL_PORT = "3306"
MYSQL_DATABASE = "test"
MYSQL_TABLE = "random_data"
TIMESTAMP_COLUMN = "created_at"
PARTITION_COLUMN_NAME = "day"
OUTPUT_DIR = "random_data/ord"

os.makedirs(OUTPUT_DIR, exist_ok=True)

duck_conn = duckdb.connect()

duck_conn.execute(f"""
INSTALL MYSQL;
LOAD MYSQL;
""")

duck_conn.execute(f"""
ATTACH 'host={MYSQL_HOST} user={MYSQL_USERNAME} port={MYSQL_PORT} db={MYSQL_DATABASE}'
AS mysqldb (TYPE MYSQL);
""")

query = f"""
COPY (SELECT *, strftime({TIMESTAMP_COLUMN}, '%Y-%m-%d') as {PARTITION_COLUMN_NAME} FROM mysqldb.{MYSQL_TABLE}) 
TO '{OUTPUT_DIR}' (FORMAT PARQUET, PARTITION_BY ({PARTITION_COLUMN_NAME}))
"""
duck_conn.execute(query)

duck_conn.close()
