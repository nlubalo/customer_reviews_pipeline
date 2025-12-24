#  Amazon Customer Reviews Data Cleaning

This document describes the **data cleaning, validation, and quality checks** applied to the **Amazon Customer Reviews dataset** before it is used for downstream **machine learning sentiment analysis**.

The objective of this pipeline is to:

- Standardize and normalize text fields
- Remove noise and low-quality records
- Derive sentiment labels
- Produce a **reliable, reproducible training dataset**
- Ensure data quality through validation checks and logging

---

## 1. Data Ingestion

- Raw data is ingested from a CSV source.
- The dataset contains:
  - **Structured fields**: `product_id`, `user_id`, `rating`
  - **Unstructured text fields**: review content, review title, product description
- All rows are preserved at ingestion time to avoid premature data loss.

---

## 2. Pre-Cleaning Data Quality Assessment

Before applying transformations, an **initial profiling step** was performed to understand data quality risks in the raw dataset.

### Identified Issues & Impact

| Issue | Description | Impact | Action |
|-----|-----------|--------|--------|
| Duplicate reviews | Same `product_id`, `user_id`, `review_id`, or near-identical text (including spam-like link variations) | Skews sentiment distribution and model learning | Remove duplicates post-normalization |
| Mixed-language text | Reviews may contain non-English or mixed-language segments (Hindi, emojis, symbols, etc.) | Adds noise to NLP models | Filter non-English segments |
| Null values | Only 2 records contain nulls in `rating_count` | Minimal impact; column unused downstream | Records retained |
| Invalid ratings | Non-numeric values (e.g. `|`) present in `rating` | Breaks sentiment derivation | Remove invalid rows |

---
## 3. Data Cleaning & Transformation Steps

### 3.1 Language Filtering (English-only Training)

- Reviews may contain **mixed-language content within a single field**
- Only **English text is retained**
- Non-English segments are removed rather than discarding entire reviews

---

### 3.2 Link Detection & Removal

- Review text is scanned for:
  - URLs
  - Embedded product links
- All detected links are removed to reduce spam and bias

---

### 3.3 Text Standardization

The following columns are standardized:

- `review_content`
- `review_title`
- `about_product`

**Applied transformations:**

- Cast values to string
- Convert text to lowercase
- Remove HTML tags
- Remove URLs and links
- Remove special characters and punctuation
- Normalize whitespace
- Trim leading and trailing spaces

---

### 3.4 Deduplication

After text normalization, duplicate reviews are removed using the combination of:

- `product_id`
- `user_id`
- Cleaned `review_content`

This ensures duplicates are detected based on **semantic content**, not raw text noise.

---


## 4. Sentiment Annotation

The dataset does not contain an explicit sentiment label. Sentiment is therefore **derived from the numeric rating** using a rule-based approach.

### 4.1 Rating Normalization

- `rating` is coerced to a numeric type
- Invalid or malformed values are converted to `NaN`
- Rows with invalid ratings are excluded from annotation

---

### 4.2 Sentiment Mapping Rules

| Rating Range | Sentiment |
|------------|----------|
| 1.0 ≤ rating < 3.0 | negative |
| 3.0 ≤ rating < 4.0 | neutral |
| 4.0 ≤ rating ≤ 5.0 | positive |

---


### 4.3 Unknown Handling

- Ratings outside the expected range
- Missing or null values

➡ Result in `NULL / None` sentiment labels rather than hard failures.

---

## 5. Running the Pipeline

### Prerequisites
- Docker
- Docker Compose
- Make

### Commands

```bash
# Initialize Airflow database and admin user
make init

# Start Airflow services
make up

# Stop services
make down

# View logs
make logs

# Bash into the Airflow webserver
make bash

# Launch Jupyter Lab
make jupyter


6. Output
The final output is a cleaned, deduplicated, English-only, sentiment-labeled dataset suitable for:
Sentiment classification













# How to Run the Pipeline
=======================


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
