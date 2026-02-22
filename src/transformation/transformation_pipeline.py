import json
import pandas as pd
from fx_conversion import load_fx_rates
from Normalize_Transform import normalize_order, normalize_items, convert_to_naira


# CONFIGURATION: paths and output filenames

RAW_DATA_PATH = "output\\commercepulse.needed_columns.json"
FX_RATES_PATH = "data\\fx_rates_2023.csv"

OUTPUT_ORDERS = "clean_orders.csv"
OUTPUT_ITEMS = "clean_items.csv"


# 1. (Safe Doc) HELPER FUNCTION: we will create a helper function to ensure that the input document is always a dictionary, even if it's a string (live event) or an unexpected type, allowing us to handle live events and historical data uniformly.
#

def safe_doc(doc):
    """
    Ensure that doc is always a dict.
    If it's a string (live event), parse as JSON.
    """
    if isinstance(doc, str):
        try:
            return json.loads(doc)
        except json.JSONDecodeError:
            print(f"Skipping invalid JSON: {doc}")
            return None
    elif isinstance(doc, dict):
        return doc
    else:
        print(f"Skipping unexpected type: {type(doc)}")
        return None


# 2. Load Raw Data: we will load the raw data from a JSON file, ensuring that we handle empty files and parse the content correctly, supporting both single JSON objects and lists of JSON objects for flexibility in our input data format.
def load_raw_data(path):
    with open(path, "r", encoding="utf-8") as f:
        content = f.read().strip()
        if not content:
            raise ValueError("Raw data file is empty")

        data = json.loads(content)
        if isinstance(data, dict):
            data = [data]
        return data



# TRANSFORMATION PIPELINE

def run_pipeline():
    print("Starting normalization pipeline...")

    raw_data = load_raw_data(RAW_DATA_PATH)
    fx_lookup = load_fx_rates(FX_RATES_PATH)

    orders = []
    items = []
    seen_event_ids = set()  # idempotency

    for doc in raw_data:
        doc = safe_doc(doc)
        if not doc:
            continue  # skip invalid records

        try:
            order = normalize_order(doc, fx_lookup)
            event_id = order["event_id"]

            # Skip duplicates
            if event_id in seen_event_ids:
                continue
            seen_event_ids.add(event_id)

            orders.append(order)

            line_items = normalize_items(doc, fx_lookup)
            items.extend(line_items)

        except Exception as e:
            print(f"Skipping record: {e}")

    # ================================
    # EXPORT CSV
    # ================================
    df_orders = pd.DataFrame(orders)
    df_items = pd.DataFrame(items)

    df_orders.to_csv(OUTPUT_ORDERS, index=False)
    df_items.to_csv(OUTPUT_ITEMS, index=False)

    print("Pipeline completed successfully")
    print(f"Orders: {len(df_orders)}")
    print(f"Items: {len(df_items)}")


if __name__ == "__main__":
    run_pipeline()