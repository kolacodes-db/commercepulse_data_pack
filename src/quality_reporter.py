# src/quality_reporter.py
import pandas as pd
import logging

logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")

def generate_daily_quality_report(df: pd.DataFrame, report_file: str):
    logging.info("Generating daily data quality report...")

    # Ensure event_date is datetime.date
    if 'event_date' in df.columns:
        df['event_date'] = pd.to_datetime(df['event_date'], errors='coerce').dt.date
    else:
        raise ValueError("Missing 'event_date' column in cleaned data")

    # Ensure numeric columns exist
    for col in ['quantity', 'sales_total_ngn', 'total_amount_ngn']:
        if col not in df.columns:
            df[col] = 0

    # Daily summary: aggregate by event_date
    daily_summary = (
        df.groupby('event_date')
        .agg(
            total_orders=('order_id', 'nunique'),
            daily_revenue_ngn=('sales_total_ngn', 'sum'),
            avg_order_value_ngn=('total_amount_ngn', 'mean'),
            total_items=('quantity', 'sum')
        )
        .reset_index()
    )

    # Save reports
    df.to_csv(report_file, index=False)
    daily_summary.to_csv(report_file.replace(".csv", "_daily_summary.csv"), index=False)

    logging.info(f"Quality report saved: {report_file}")
    logging.info(f"Daily summary saved: {report_file.replace('.csv', '_daily_summary.csv')}")

    # Log sample
    logging.info("\n Sample Daily Summary:\n" + str(daily_summary.head()))