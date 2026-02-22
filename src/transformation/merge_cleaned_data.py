import pandas as pd

orders = pd.read_csv("output/clean_orders.csv")
items = pd.read_csv("output/clean_items.csv")

cleaned_data = items.merge(
    orders,
    on=["event_id", "order_id"],
    how="left"
)
cleaned_data.to_csv("output/cleaned_data.csv", index=False)