
import os
import logging
from google.cloud import bigquery

logging.basicConfig(level=logging.INFO)

PROJECT_ID = os.getenv("GOOGLE_PROJECT_ID")
DATASET = "commercepulse"
STAGING_TABLE = f"{PROJECT_ID}.{DATASET}.stg_raw_data"

def get_client():
    return bigquery.Client(project=PROJECT_ID)

def read_sql_file(file_path):
    if not os.path.exists(file_path):
        raise FileNotFoundError(f"SQL file not found: {file_path}")
    with open(file_path, "r", encoding="utf-8") as f:
        return f.read()

def run_sql(client, sql_query, name=""):
    logging.info(f"Running SQL: {name}")
    job = client.query(sql_query)
    job.result()
    logging.info(f"Completed: {name}")

def run_all_sql():
    client = get_client()
    sql_folder = os.path.join("src", "Bigquery_lngestion", "sql")

    sql_files = [
        "dim.customer.sql",
        "dim.date.sql",
        "dim.product.sql",
        "fact.orders.sql",
        "fact.payments.sql",
        "fact.refunds.sql",
        "fact.shipments.sql",
        "fact.order_daily.sql"
    ]

    for sql_file in sql_files:
        sql_path = os.path.join(sql_folder, sql_file)
        sql_query = read_sql_file(sql_path)

        # Replace any placeholder for staging table in SQL
        sql_query = sql_query.replace("{{STAGING_TABLE}}", STAGING_TABLE)

        run_sql(client, sql_query, name=sql_file)

    logging.info("All SQL executed successfully!")

if __name__ == "__main__":
    run_all_sql()