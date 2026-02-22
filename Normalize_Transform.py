import hashlib
from datetime import datetime


# 1. Canonical Normalization: i created a helper function to canonicalize keys in the payload, ensuring consistent access to fields regardless of variations in naming or formatting.

def canonicalize_keys(d):
    return {k.strip().lower(): v for k, v in d.items()}


# 2. ID Generation: Generating a consistent event_id if it's missing by using a combination of order_id and event_date, ensuring that we have a unique identifier for each record even if the original event_id is missing or inconsistent.

def generate_event_id(order_id, event_date):
    raw = f"{order_id}_{event_date}"
    return hashlib.sha256(raw.encode()).hexdigest()


# 3. FX Conversion: we will convert amounts to Naira using the FX lookup during normalization, ensuring we handle missing or null values safely by treating them as zero. We will also ensure that we only apply FX conversion to amounts in USD and leave other currencies unchanged.

def convert_to_naira(amount, currency, fx_rate):
    if amount is None:
        return 0
    if currency == "USD":
        return amount * fx_rate
    return amount


# 4. Extract Dates (Live + Historical): we will extract event_date, event_time, and ingested_at with a focus on safely handling missing fields and ensuring we have a consistent event_date for FX lookup.

def extract_dates(doc):
    payload = doc.get("payload", {})

    # Use payload.created_at if exists, otherwise fallback to event_time
    event_date = payload.get("created_at") or doc.get("event_time")
    ingested_at = doc.get("ingested_at")
    event_time = doc.get("event_time")

    return event_date, event_time, ingested_at


# 5. Order Normalization: similar to line items, we will normalize orders with a focus on safely handling missing fields and FX conversion. We will also ensure that we generate a consistent event_id if it's missing by using a combination of order_id and event_date.

def normalize_order(doc, fx_lookup):
    payload = canonicalize_keys(doc.get("payload", {}))

    order_id = payload.get("order_id") or doc.get("event_id")  # use event_id if no order_id
    currency = payload.get("currencycode", "NGN")  # default NGN

    event_date, event_time, ingested_at = extract_dates(doc)
    if not event_date:
        raise ValueError(f"Missing event date for record {order_id}")

    fx_rate = fx_lookup.get(event_date[:10], 1)  # safe fx lookup

    total_amount = payload.get("totalamount") or 0
    total_amount_ngn = convert_to_naira(total_amount, currency, fx_rate)

    event_id = doc.get("event_id") or generate_event_id(order_id, event_date)

    return {
        "event_id": event_id,
        "order_id": order_id,
        "event_date": event_date,
        "created_at": ingested_at,
        "event_time": event_time,
        "customer_id": payload.get("customerid"),
        "buyer_email": payload.get("buyeremail"),
        "buyer_phone": payload.get("buyerphone"),
        "currency": currency,
        "total_amount": total_amount,
        "total_amount_ngn": total_amount_ngn,
        "state": payload.get("state"),
        "city": payload.get("city") or payload.get("address", {}).get("city"),
        "country": payload.get("country") or payload.get("address", {}).get("country"),
        "vendor": doc.get("vendor") or payload.get("vendor"),
        "event_type": doc.get("event_type") or payload.get("event_type")
    }


# 6. Line Items Normalization: similar to order normalization but for line items, ensuring we handle missing fields and FX conversion safely.

def normalize_items(doc, fx_lookup):
    payload = canonicalize_keys(doc.get("payload", {}))

    order_id = payload.get("order_id") or doc.get("event_id")
    currency = payload.get("currencycode", "NGN")
    event_date = payload.get("created_at") or doc.get("event_time")
    fx_rate = fx_lookup.get(event_date[:10], 1)
    event_id = doc.get("event_id") or generate_event_id(order_id, event_date)

    items = []

    for item in payload.get("line_items", []):
        item = canonicalize_keys(item)
        qty = item.get("quantity") or 0
        price = item.get("unit_price") or 0
        line_total = qty * price

        items.append({
            "event_id": event_id,
            "order_id": order_id,
            "sku": item.get("sku"),
            "quantity": qty,
            "unit_price": price,
            "unit_price_ngn": convert_to_naira(price, currency, fx_rate),
            "sales_total": line_total,
            "sales_total_ngn": convert_to_naira(line_total, currency, fx_rate)
        })

    return items