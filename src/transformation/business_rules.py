# business_rules.py

import pandas as pd


def apply_business_rules(df: pd.DataFrame) -> pd.DataFrame:

    if "event_time" in df.columns:
        df["event_date"] = df["event_time"].dt.date

    # Safe numeric conversion (fix for your second error)
    if "amount" in df.columns and isinstance(df["amount"], pd.Series):
        df["amount"] = pd.to_numeric(df["amount"], errors="coerce")

    if "refund_amount" in df.columns and isinstance(df["refund_amount"], pd.Series):
        df["refund_amount"] = pd.to_numeric(df["refund_amount"], errors="coerce")

    if "amount" in df.columns and "refund_amount" in df.columns:
        df["net_amount"] = (
            df["amount"].fillna(0) -
            df["refund_amount"].fillna(0)
        )

    if "event_time" in df.columns:
        yesterday = pd.Timestamp.utcnow().normalize() - pd.Timedelta(days=1)
        df["is_late"] = df["event_time"] < yesterday

    if "event_type" in df.columns:
        df["is_order_event"] = df["event_type"].str.contains("order", na=False)
        df["is_payment_event"] = df["event_type"].str.contains("payment", na=False)
        df["is_refund_event"] = df["event_type"].str.contains("refund", na=False)
        df["is_shipment_event"] = df["event_type"].str.contains("shipment", na=False)

    if "customer_id" in df.columns:
        df["customer_key"] = df["customer_id"]

    if "product_id" in df.columns:
        df["product_key"] = df["product_id"]

    # Ensure date_key is datetime
        df["date_key"] = pd.to_datetime(df["date_key"], errors="coerce")  # invalid parsing becomes NaT

    # Now you can format it safely
        df["date_key"] = df["date_key"].dt.strftime("%Y%m%d")

    rename_map = {
        "buyeremail": "buyer_email",
        "buyerphone": "buyer_phone",
        "customerid": "customer_id",
        "totalamount": "total_amount",
        "currencycode": "currency",
        "updatedat": "updated_at",
    }

    df.rename(columns={k: v for k, v in rename_map.items() if k in df.columns}, inplace=True)
    return df
