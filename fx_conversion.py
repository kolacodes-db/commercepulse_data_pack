def load_fx_rates(path):
    import pandas as pd

    df = pd.read_csv(path)

   
    # 1. Canonicalize column names: strip whitespace and convert to lowercase to handle variations in FX rate file formatting and ensure consistent access to columns in our transformation pipeline.
    
    df.columns = [c.strip().lower() for c in df.columns]

    
    # 2. Map possible FX column names: there is no standard naming convention for FX rates, so we will look for common variations and map them to a standard "rate" column for consistency in our transformation pipeline.
   
    column_mapping = {
        "usdngn": "rate",
        "fx_rate": "rate",
        "exchange_rate": "rate",
        "usd_to_ngn": "rate"
    }

    df = df.rename(columns=column_mapping)

  
    # 3. Validate required columns: ensure 'date' and 'rate' columns exist after mapping
   
    if "date" not in df.columns:
        raise ValueError("FX file must contain 'date' column")

    if "rate" not in df.columns:
        raise ValueError(f"No valid FX rate column found. Columns: {df.columns}")


    # 4. Normalize date format: ensure it's YYYY-MM-DD for consistent lookup during transformation
   
    df["date"] = pd.to_datetime(df["date"]).dt.strftime("%Y-%m-%d")

   
    # 5. Build lookup dictionary: the FX lookup will be a simple dict with date as key and rate as value for easy access during transformation
 
    fx_lookup = dict(zip(df["date"], df["rate"]))

    return fx_lookup

import json

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