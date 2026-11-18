"""
Automated Retail Orders ETL Pipeline
Internship Track: Week 3 - Task 5
Author: Sayantani Adhikary
"""

import logging
import sqlite3
import pandas as pd

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s"
)


def extract(source_path: str) -> pd.DataFrame:
    """Extract raw order records from CSV source."""
    logging.info(f"Extracting data from {source_path}...")
    df = pd.read_csv(source_path)
    logging.info(f"Extracted {len(df)} raw records.")
    return df


def transform(df: pd.DataFrame) -> pd.DataFrame:
    """Clean, standardize, impute, and enrich order records."""
    logging.info("Starting data transformation...")

    before_dedup = len(df)
    df = df.drop_duplicates(subset=["OrderID"]).copy()
    logging.info(f"Removed {before_dedup - len(df)} duplicate record(s).")

    df["Region"] = df["Region"].str.strip().str.title()
    df["Status"] = df["Status"].str.strip().str.title()
    df = df[df["Status"] == "Completed"].copy()

    df["Quantity"] = df["Quantity"].fillna(df["Quantity"].median()).astype(int)
    df["UnitPrice"] = df["UnitPrice"].fillna(df["UnitPrice"].median()).round(2)
    df["TotalAmount"] = (df["Quantity"] * df["UnitPrice"]).round(2)

    logging.info(f"Transformation complete. {len(df)} clean records ready.")
    return df


def load(df: pd.DataFrame, csv_dest: str, db_dest: str) -> None:
    """Load transformed data into CSV and SQLite destination."""
    df.to_csv(csv_dest, index=False)
    logging.info(f"Loaded processed data to CSV: {csv_dest}")

    with sqlite3.connect(db_dest) as conn:
        df.to_sql("processed_orders", conn, if_exists="replace", index=False)
    logging.info(f"Loaded processed data to SQLite table 'processed_orders' in {db_dest}")


def run_pipeline():
    raw_df = extract("data/raw_orders.csv")
    clean_df = transform(raw_df)
    load(clean_df, "data/processed_orders.csv", "data/orders_warehouse.db")
    logging.info("ETL Pipeline executed successfully!")


if __name__ == "__main__":
    run_pipeline()
