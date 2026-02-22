import pandas as pd
from aggregation import (
    normalize_event_date,
    aggregate_orders,
    aggregate_items,
    generate_date_spine
)


# LOAD CLEAN DATA (CSV): input to the report generator

orders = pd.read_csv("clean_orders.csv")
items = pd.read_csv("clean_items.csv")



# NORMALIZE DATES: extract date from event_date and ensure it's in proper datetime format

orders = normalize_event_date(orders)



# DATE RANGE: determine the date range for the report based on the orders data

start_date = orders["date"].min()
end_date = orders["date"].max()



# BUILD DATE SPINE: create a complete date range to ensure we have rows for all dates, even those with no orders/items

date_spine = generate_date_spine(start_date, end_date)



# AGGREGATIONS: compute daily totals for orders and items

orders_daily = aggregate_orders(orders)
items_daily = aggregate_items(items, orders)



# MERGE REPORT: combine date spine with orders and items summaries to create the final report structure

report = date_spine.merge(orders_daily, on="date", how="left")
report = report.merge(items_daily, on="date", how="left")

# Fill missing days
report = report.fillna(0)



# EXPORT FINAL REPORT: save the daily report as a CSV file

report.to_csv("daily_report.csv", index=False)

print("Daily report generated successfully")
