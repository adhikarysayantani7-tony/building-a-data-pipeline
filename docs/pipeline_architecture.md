# Data Pipeline Architecture & Functionality Documentation

## 1. Overview
This document describes the design, data flow, and orchestration of an automated **ETL (Extract, Transform, Load)** pipeline built to process retail order records. The pipeline eliminates manual data cleaning, enforces schema consistency, and loads validated data into both flat-file and relational storage.

---

## 2. Architecture Flow

```text
[ data/raw_orders.csv ]
          │
          ▼  (1. EXTRACT)
[ src/etl_pipeline.py :: extract() ]
          │
          ▼  (2. TRANSFORM)
  • Deduplicate OrderID
  • Standardize Region & Status casing
  • Filter out Cancelled orders
  • Median-impute missing Quantity & UnitPrice
  • Compute TotalAmount = Quantity * UnitPrice
          │
          ▼  (3. LOAD)
[ data/processed_orders.csv ]  +  [ data/orders_warehouse.db (SQLite) ]
