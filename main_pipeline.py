import logging
import pandas as pd
import os

# Transformation
from src.transformation.transform_data import run_pipeline

# Load
from src.bigquery_lngestion.bigquery_loader import load_cleaned_to_bq

# Quality Report
from src.quality_reporter import generate_daily_quality_report

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s"
)

def main_pipeline():
    try:
        logging.info("🚀 Starting Full Data Pipeline...")

      
        # File paths
   
        input_file = "output/clean_data.csv"  # your input CSV
        cleaned_file = "output/cleaned_final_data.csv"
        report_file = "output/daily_quality_report.csv"
        fx_file = "data/fx_rates_2023.csv"  # if you use FX conversion

     
        # STEP 1: TRANSFORMATION: we will run the transformation pipeline to clean and normalize the raw data, ensuring that we handle any potential issues with the input data and confirm that the cleaned dataset is generated successfully before proceeding to the next steps.
      
        logging.info("🔄 Running Transformation Step...")
        df_cleaned = run_pipeline(input_file, cleaned_file, fx_file)  # returns a cleaned DataFrame

        if df_cleaned.empty:
            raise ValueError("Cleaned dataset is empty. Pipeline stopped.")
        logging.info(f"Transformation complete: {len(df_cleaned)} rows")

       
        # STEP 2: LOAD TO BIGQUERY: we will load the cleaned data into BigQuery, ensuring that we handle any potential issues with the data format and confirm successful ingestion before proceeding to the quality reporting step.
    
        logging.info("Loading data to BigQuery...")
        load_cleaned_to_bq(cleaned_file)
        logging.info("BigQuery load complete")

      
        # STEP 3: DATA QUALITY REPORT: we will generate a daily data quality report from the cleaned dataset, summarizing key metrics such as total orders, daily revenue, average order value, and total items sold, and save this report to a CSV file for further analysis and monitoring.
      
        logging.info("Generating daily data quality report...")
        generate_daily_quality_report(df_cleaned, report_file)
        logging.info(f"Quality report saved to {report_file}")

        logging.info(" PIPELINE EXECUTED SUCCESSFULLY")

    except Exception as e:
        logging.error(f"Pipeline failed: {e}", exc_info=True)

if __name__ == "__main__":
    main_pipeline()