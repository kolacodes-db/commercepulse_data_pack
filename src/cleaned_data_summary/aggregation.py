import pandas as pd



# Normalize event_date properly

def normalize_event_date(df):

    df = df.copy()

    df["event_date"] = pd.to_datetime(df["event_date"], errors="coerce", utc=True)

    # Extract DATE only (canonical): YYYY-MM-DD
    df["date"] = df["event_date"].dt.date

    return df



# Orders aggregation: total orders and revenue per day

def aggregate_orders(df):

    return df.groupby("date").agg(
        total_orders=("order_id", "nunique"),
        total_revenue=("total_amount_ngn", "sum")
    ).reset_index()



# Items aggregation: total quantity and revenue per day

def aggregate_items(items_df, orders_df):

    merged = items_df.merge(
        orders_df[["order_id", "date"]],
        on="order_id",
        how="left"
    )

    return merged.groupby("date").agg(
        total_items=("quantity", "sum"),
        total_item_revenue=("sales_total_ngn", "sum")
    ).reset_index()



# Date spine : generate a complete date range for the report

def generate_date_spine(start_date, end_date):

    return pd.DataFrame({
        "date": pd.date_range(start=start_date, end=end_date).date
    })