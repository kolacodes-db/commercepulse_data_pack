# commercepulse_data_pack
The is the third semester examination for Alt_school_africa data_engineering class

~  addition of readme.md file
project overview
The CommercePulse Data Pack is a full data pipeline for processing, cleaning, and analyzing e-commerce transaction data. It takes raw order and line item data, applies transformations, normalizes currencies, and generates clean datasets ready for storage in BigQuery. Additionally, the pipeline produces a daily quality report that summarizes key metrics such as total orders, revenue, average order value, and total items sold per day.

This project is designed for reliability, scalability, and easy debugging. Malformed or incomplete records are automatically handled without breaking the pipeline  
commercepulse_data_pack/
│
├── main_pipeline.py                # Entry point to run full pipeline
├── output/                         # Output folder for processed files
│   ├── cleaned_final_data.csv
│   ├── daily_quality_report.csv
│   └── daily_quality_report_daily_summary.csv
│
├── src/                            # Source code folder
│   ├── transformation/             # Data cleaning and transformation
│   │   ├── transform_data.py       # Main transformation script
│   │   └── Normalize_Transform.py # Helper functions for normalizing orders/items and FX
│   │
│   ├── bigquery_lngestion/         # BigQuery loading scripts
│   │   └── bigquery_loader.py
│   │
│   └── quality_reporter.py         # Generates daily quality reports
│
├── venv/                           # Python virtual environment
│
├── requirements.txt                # Python dependencies
├── README.md                        # Project instructions
└── .gitignore                      # Files to ignore for Git

Features

Data Transformation: Cleans and normalizes raw order and line item data.

Currency Conversion: Converts amounts to a base currency using foreign exchange rates.

BigQuery Upload: Automatically uploads cleaned data to a BigQuery table.

Daily Quality Report: Produces CSV reports with daily totals for orders, revenue, and items sold.

Error Handling: Skips malformed records and logs warnings without stopping the pipeline.

Flexible Input: Supports CSV files for raw data and FX rates.

Data Schema

The pipeline expects raw data with the following columns:

event_id

order_id

sku

quantity

unit_price

unit_price_ngn

sales_total

sales_total_ngn

event_date

customer_id

buyer_email

buyer_phone

currency

total_amount

total_amount_ngn

state

city

country

vendor

event_type

Installation

Clone the repository:

git clone https://github.com/yourusername/commercepulse_data_pack.git
cd commercepulse_data_pack

Create and activate a Python virtual environment:

python -m venv venv
source venv/bin/activate   # Linux or Mac
venv\Scripts\activate      # Windows

Install dependencies:

pip install -r requirements.txt
Usage
Running the Full Pipeline
python main_pipeline.py

output files
python -m src.transformation.transform_data input/raw_data.csv output/cleaned_final_data.csv input/fx_rates.csv
Output
Cleaned Data

File: output/cleaned_final_data.csv

Contains cleaned and normalized order and line item data

Daily Quality Report

File: output/daily_quality_report.csv

File: output/daily_quality_report_daily_summary.csv

Daily summary example:

event_date	total_orders	daily_revenue_ngn	avg_order_value_ngn	total_items
2023-01-03	1	175800	58600	8
2023-01-05	1	4000	4000	1
2023-01-08	1	249824	83275	6
Charting

Y

work flow_chart
Start
  |
  v
[Local Development]
  |
  |---> Edit Code / Transform Scripts / Quality Reporter
  | 
  v
[Stage Changes]
  git add .
  |
  v
[Commit Changes]
  git commit -m "Descriptive commit message"
  |
  v
[Check Remote Branch]
  git fetch origin
  |
  v
[Resolve Conflicts if any]
  git pull origin main
  (resolve merge conflicts)
  git add .
  git commit -m "Resolve merge conflicts"
  |
  v
[Push Changes to GitHub]
  git push origin main
  |
  v
[Verify on GitHub]
  |
  v
[Pipeline Execution]
  |
  |---> Run main_pipeline.py
  |       |
  |       v
  |  Load Input CSV
  |       |
  |       v
  |  Transform Data
  |       |
  |       v
  |  Normalize Orders & Items
  |       |
  |       v
  |  Apply FX Rates (if available)
  |       |
  |       v
  |  Save Cleaned CSV
  |       |
  |       v
  |  Load to BigQuery
  |       |
  |       v
  |  Generate Quality Report
  |
  v
[Success / Review Outputs]
  - Check BigQuery table
  - Check CSV outputs
  - Check daily_quality_report.csv
  |
  v
[Loop for Next Changes]
  |
  v

End


Trade-Off Analysis
This section outlines the key architectural and engineering decisions made during the development of the data pipeline and warehouse system, with careful consideration of performance, scalability, correctness, and maintainability.

1. Historical Batch Processing vs Live Event Handling
The method used in the execution of this project adopts a historical batch processing approach rather than real-time event streaming. This is because live event handling introduces significant complexity in terms of infrastructure, monitoring, and fault tolerance. On the other hand, presents advantages such as the availability of structured historical datasets (CSV exports), Simpler implementation and debugging, Reduced infrastructure overhead and suitability for analytical workloads such as trend analysis and reporting. Although, this approach introduces latency, as data is only updated periodically rather than in real time. 

2. MongoDB vs Google BigQuery Responsibilities
There is a clear separation of responsibilities differences maintained between the operational database(MongoDB) and the analytical warehouse (BigQuery). MongoDB serves as the source system, data lake for raw data ingestion, flexible schema design and high velocity transactional ingestion while BigQuery serves as the analytical layer, optimized for columnar storage and fast aggregation queries, structured and normalized schema design and partitioned and scalable data processing The trade-off lies in the need for transformation between semi-structured and structured formats. While MongoDB offers flexibility, there is additional transformation steps before data becomes analytics-ready in BigQuery.

3. Append-Only vs Upsert Strategies
An append-only strategy was adopted for loading data into fact tables, as opposed to using upserts (MERGE operations). Append-only was adopted for simpler pipeline logic, improved performance in BigQuery, avoids costly row-level updates and reduced risk of accidental data overwrites. While Upsert in such situations may lead to increased computational cost, added complexity in handling late-arriving data and reduced query performance at scale

4. Pandas vs SQL Transformations
Data transformation was performed using a hybrid approach involving Python (Pandas) and SQL within BigQuery. Pandas was used for Initial CSV ingestion and preprocessing, Parsing and flattening nested JSON fields (items) and Handling inconsistent schemas (e.g., unit_price vs price, quantity vs qty) while SQL (BigQuery) transformation was used for Data modeling (fact and dimension tables), Aggregations and filtering, Partitioning and query optimization. This is because Pandas offers flexibility and fine-grained control but is memory-bound and less scalable while SQL is highly scalable and optimized for large datasets but less flexible for complex data cleaning.

5. Correctness vs Performance
A deliberate balance was struck between ensuring data correctness and achieving optimal performance.Priority was given to correctness during transformation in Handling inconsistent JSON schemas, Ensuring accurate extraction of product_id, price, and quantity and avoiding null or malformed records while Performance optimizations were applied at the warehouse level for Partitioning tables using DATE(event_time), Selecting only relevant columns and avoiding unnecessary joins and recomputations

6. Simplicity vs Long-Term Maintainability
The system design prioritizes simplicity in the short term, while maintaining a rigid premise for future scalability. Simplicity was achieved through clear separation of ETL stages (Extract → Transform → Load), use of straightforward SQL (CREATE OR REPLACE, SELECT DISTINCT) and avoidance of overly complex orchestration tools while longterm maintainability was adopted in normalized schema design (fact and dimension tables), modular SQL scripts and reusable transformation logic. However, the current design remains extensible and can evolve into a more robust system if required.


After transformation, the pipeline produces a cleaned CSV with the same columns relevant for analysis.~   
