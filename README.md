# 🧹 Amazon Customer Reviews Data Cleaning

This document describes the data cleaning, validation, and quality checks applied to the **Amazon Customer Reviews Datasett** before it is used for downstream machine learning sentiment analysis.

The goal of this pipeline is to **standardize text, annotate data, enforce data quality without hard failures, and produce a reliable training dataset**.

---

## 1. Data Ingestion

- Raw data is ingested from a CSV file.
- The dataset contains structured fields (e.g. `product_id`, `user_id`, `rating`) and unstructured text fields (e.g. review content and titles).
- Initial ingestion preserves all rows to avoid early data loss.


# 1. Initialize Airflow DB and admin
make init

# 2. Start Airflow
make up

make down

# 3. Check logs if needed
make logs

# 4. Bash into webserver
make bash

# 5. Launch Jupyter Lab
make jupyter
