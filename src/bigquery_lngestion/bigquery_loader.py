import os
import pandas as pd
from pandas_gbq import to_gbq
from dotenv import load_dotenv

load_dotenv()  # ensure GOOGLE_PROJECT_ID is set

# Defining the BigQuery staging table
STAGING_TABLE = "commercepulsedatapack.commercepulse.cleaned_data"

NEEDED_COLUMNS = [
    "event_id",
    "order_id",
    "sku",
    "quantity",
    "unit_price",
    "unit_price_ngn",
    "sales_total",
    "sales_total_ngn",
    "event_date",
    "created_at",
    "event_time",
    "customer_id",
    "buyer_email",
    "buyer_phone",
    "currency",
    "total_amount",
    "total_amount_ngn",
    "state",
    "city",
    "country",
    "vendor",
    "event_type"
]

def load_cleaned_to_bq(file_path: str):
  
    # Load CSV/JSON: support both formats, ensure all columns are present, and handle missing values
   
    if file_path.endswith(".csv"):
        df = pd.read_csv(file_path, dtype=str)
    elif file_path.endswith((".json", ".jsonl")):
        df = pd.read_json(file_path, lines=True, dtype=str)
    else:
        raise ValueError("Unsupported file type. Use CSV or JSON/JSONL.")

   
    # Ensure all columns exist: if missing, create them with default values
  
    df = df.reindex(columns=NEEDED_COLUMNS, fill_value="Not Applicable")

  
    # Fill string columns: replace NaN with "Not Applicable" and ensure all are strings
  
    str_cols = ["event_id","order_id","sku","customer_id","buyer_email","buyer_phone",
                "currency","state","city","country","vendor","event_type"]
    for col in str_cols:
        df[col] = df[col].fillna("Not Applicable").astype(str)

   
    # Fill numeric columns: convert to numeric, coerce errors to NaN, and fill NaN with 0
   
    num_cols = ["quantity","unit_price","unit_price_ngn","sales_total","sales_total_ngn",
                "total_amount","total_amount_ngn"]
    for col in num_cols:
        df[col] = pd.to_numeric(df[col], errors="coerce").fillna(0)

    
    # Format datetime columns: YYYY-MM-DD HH:MM:SS; handle parsing errors and fill invalid dates with "Not Applicable"
  
    date_cols = ["event_date","created_at","event_time"]
    for col in date_cols:
        # Parse datetime safely
        df[col] = pd.to_datetime(df[col], errors="coerce", utc=True)
        # Convert to string format YYYY-MM-DD HH:MM:SS
        df[col] = df[col].dt.strftime("%Y-%m-%d %H:%M:%S")
        # Fill any remaining NaT as "Not Applicable"
        df[col] = df[col].fillna("Not Applicable")

   
    # Load all rows to BigQuery (no filtering by event_id): Only 327 data was loaded if event_id is filtered, so we will load all data and let BigQuery handle duplicates if needed. This ensures we don't miss any records due to event_id issues.
  
    project = os.getenv("GOOGLE_PROJECT_ID")
    to_gbq(
        df,
        destination_table=STAGING_TABLE,
        project_id=project,
        if_exists="replace",
        progress_bar=True
    )

    print(f"{len(df)} rows uploaded to {STAGING_TABLE}")

if __name__ == "__main__":
    FILE_PATH = "output/cleaned_data.csv"
    load_cleaned_to_bq(FILE_PATH)