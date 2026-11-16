# Building a Data Pipeline

## Project Overview
This repository implements an automated **Extract, Transform, Load (ETL)** data pipeline in Python orchestrated by **Apache Airflow**. It extracts messy retail order records, cleans duplicates and missing values, enriches revenue totals, and loads the validated data into CSV and SQLite destinations.

## Repository Structure
- `README.md`: Project overview, pipeline summary, and setup guide
- `requirements.txt`: Python dependencies (`pandas`, `apache-airflow`)
- `data/raw_orders.csv`: Raw source order records
- `data/processed_orders.csv`: Cleaned, transformed output dataset
- `src/etl_pipeline.py`: Modular Python ETL script (`extract()`, `transform()`, `load()`)
- `dags/orders_etl_dag.py`: Apache Airflow DAG scheduling daily automated runs
- `docs/pipeline_architecture.md`: Detailed architecture diagram and stage-by-stage documentation

## ETL Pipeline Stages
1. **Extract**: Reads raw transactional records from `data/raw_orders.csv`.
2. **Transform**: Deduplicates `OrderID`, standardizes `Region` and `Status` text casing, filters out `Cancelled` orders, median-imputes missing `Quantity` and `UnitPrice` values, and computes `TotalAmount`.
3. **Load**: Exports clean data to `data/processed_orders.csv` and loads it into SQLite (`data/orders_warehouse.db`).

## How to Run
1. Clone the repository:
   `git clone https://github.com/adhikarysayantani7-tony/building-a-data-pipeline.git`
2. Install dependencies:
   `pip install -r requirements.txt`
3. Run the ETL pipeline:
   `python src/etl_pipeline.py`
