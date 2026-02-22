# src/transformation/transform_data.py
import pandas as pd
import logging

logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")


# FX Utilities (internal)

def load_fx_rates(fx_file: str):
    """Load FX rates CSV: columns [date, rate]. Returns dict of {date: rate}."""
    try:
        df_fx = pd.read_csv(fx_file)
        df_fx['date'] = pd.to_datetime(df_fx['date']).dt.date
        fx_lookup = dict(zip(df_fx['date'], df_fx['rate']))
        return fx_lookup
    except Exception as e:
        logging.warning(f"⚠️ Failed to load FX rates: {e}. Defaulting to 1.")
        return {}

def convert_to_ngn(amount, date, fx_lookup):
    """Convert given amount to NGN using FX lookup."""
    try:
        rate = fx_lookup.get(date, 1)
        return amount * rate
    except Exception:
        return 0


# Normalization Functions

def normalize_order(row, fx_lookup):
    """Extract order-level info."""
    event_date = pd.to_datetime(row.get('event_date', None), errors='coerce').date()
    total_amount_ngn = convert_to_ngn(row.get('total_amount', 0), event_date, fx_lookup)
    return {
        'event_id': row.get('event_id'),
        'order_id': row.get('order_id'),
        'event_date': event_date,
        'total_amount_ngn': total_amount_ngn,
        'customer_id': row.get('customer_id'),
        'currency': row.get('currency', 'NGN')
    }

def normalize_items(row, fx_lookup):
    """Extract line-item info."""
    try:
        quantity = int(row.get('quantity', 0))
        unit_price = float(row.get('unit_price', 0))
        event_date = pd.to_datetime(row.get('event_date', None), errors='coerce').date()
        sales_total_ngn = convert_to_ngn(quantity * unit_price, event_date, fx_lookup)
        return [{
            'event_id': row.get('event_id'),
            'order_id': row.get('order_id'),
            'sku': row.get('sku'),
            'quantity': quantity,
            'unit_price': unit_price,
            'sales_total_ngn': sales_total_ngn
        }]
    except Exception as e:
        logging.warning(f"Skipped malformed line item: {e}")
        return []


# Pipeline Runner

def run_pipeline(input_file: str, cleaned_file: str, fx_file: str):
    logging.info("Running Transformation Pipeline...")
    
    # Load input CSV
    try:
        df_raw = pd.read_csv(input_file)
        logging.info(f"Input file loaded: {len(df_raw)} rows")
    except Exception as e:
        raise ValueError(f"Failed to load input file: {e}")
    
    # Load FX rates
    fx_lookup = load_fx_rates(fx_file)
    
    orders = []
    items = []

    for idx, row in df_raw.iterrows():
        try:
            orders.append(normalize_order(row, fx_lookup))
            items.extend(normalize_items(row, fx_lookup))
        except Exception as e:
            logging.warning(f"Skipped record at index {idx}: {e}")
    
    df_orders = pd.DataFrame(orders)
    df_items = pd.DataFrame(items)
    
    # Merge order-level info into items for reporting
    df_cleaned = pd.merge(
        df_items,
        df_orders[['event_id', 'order_id', 'event_date', 'total_amount_ngn']],
        on=['event_id', 'order_id'],
        how='left'
    )

    # Ensure numeric columns
    df_cleaned['quantity'] = pd.to_numeric(df_cleaned['quantity'], errors='coerce').fillna(0)
    df_cleaned['sales_total_ngn'] = pd.to_numeric(df_cleaned['sales_total_ngn'], errors='coerce').fillna(0)
    df_cleaned['total_amount_ngn'] = pd.to_numeric(df_cleaned['total_amount_ngn'], errors='coerce').fillna(0)

    # Save cleaned CSV
    df_cleaned.to_csv(cleaned_file, index=False)
    logging.info(f"Transformation complete. Cleaned data saved to {cleaned_file} ({len(df_cleaned)} rows)")

    return df_cleaned


# CLI Runner

if __name__ == "__main__":
    import sys
    if len(sys.argv) != 4:
        print("Usage: python transform_data.py <input_csv> <cleaned_csv> <fx_csv>")
        sys.exit(1)
    
    input_csv = sys.argv[1]
    cleaned_csv = sys.argv[2]
    fx_csv = sys.argv[3]
    
    run_pipeline(input_csv, cleaned_csv, fx_csv)