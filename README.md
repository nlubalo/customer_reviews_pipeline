# 🧹 Amazon Customer Reviews Data Cleaning

This document describes the data cleaning, validation, and quality checks applied to the **Amazon Customer Reviews Datasett** before it is used for downstream machine learning sentiment analysis.

The goal of this pipeline is to **standardize text, annotate data, enforce data quality without hard failures, and produce a reliable training dataset**.

---

## 1. Data Ingestion

---

- Raw data is ingested from a CSV file.
- The dataset contains structured fields (e.g. `product_id`, `user_id`, `rating`) and unstructured text fields (e.g. review content and titles).
- Initial ingestion preserves all rows to avoid early data loss.

## 2. Data Issues Identified (Pre-Cleaning Assessment)
Before applying any cleaning or transformation logic, an **initial data profiling step** was performed to understand the quality and risks present in the raw reviews dataset.

Below are the **observed data issues**, their impact, and why each required explicit handling in the cleaning pipeline.

#### 1. Duplicate Reviews

- Multiple reviews share the same:
  - `product_id`
  - `user_id`
  - `review_id`
  - Review text (or near-identical text) - Some of the duplicate products have exact same review text and others have near duplicates with variations 'spam' like product links

#### 2. Mixed-Language Reviews (Non-English Content)

- Review text may contain:
  - Fully non-English reviews
  - Mixed-language reviews within a single field
- Languages are **not limited to a known set**
  - e.g. Hindi, regional scripts, emojis, symbols


#### 4. Null Rows
- The dataset doesn't have a large number of missing values. Only two records have null
in the `rating_count` column
- Since the number of null rows is very small, these records can be safely removed without impacting the dataset quality. However,given that the `rating_count` column is not used in the downstream task, this step is optional and so I chose to keep these records in the dataset.

#### 5. Non numerical value in rating column
- The rating column is meant to be numerical in a range of 1-5. However, there are some records with non numerical values such as '|`
- These records need to be removed to ensure data quality.



## 3. Data Cleaning Steps

---

The following cleaning steps are applied:

#### 1. Language Filtering (English-only Training)
- Since all reviews of a product are concatenated together, theReviews may contain **mixed-language content within a single field**
- Only **English text is retained for model training**
- Non-English segments are removed rather than dropping the entire review

#### 2. Link Detection & Removal
- Review content is checked for embedded URLs or product links
- Any detected links are removed from the text

### 3. Text Standardization
The following text columns are standardized:
- `review_content`
- `review_title`
- `about_product`

For each text column, the following transformations are applied:
- Convert values to string
- Convert text to lowercase
- Remove HTML tags
- Remove URLs and links
- Remove special characters and punctuation
- Normalize whitespace
- Trim leading and trailing spaces

#### 4 Drop Duplicates
With the review text cleaned and standardized, duplicates are identified and removed:
- Exact duplicate reviews are removed based on the combination of:
  - `product_id`
  - `user_id`
  - Cleaned `review_content` text

#### 5 Sentiment Annotation
The dataset does not contain an explicit sentiment label. Sentiment is therefore **derived from the numeric rating** using a simple, rule-based approach.

#### Annotation Steps

1. **Rating normalization**
   - The `rating` column is converted to a numeric type.
   - Invalid or malformed values are coerced to `NaN` to prevent incorrect labeling.

2. **Rule-based sentiment mapping**
   Each review is assigned a sentiment label based on its star rating:

   | Rating Range            | Sentiment |
   |-------------------------|-----------|
   | 1.0 ≤ rating < 3.0      | negative  |
   | 3.0 ≤ rating < 4.0      | neutral   |
   | 4.0 ≤ rating ≤ 5.0      | positive  |

3. **Unknown handling**
   - Ratings outside the expected range or missing values result in a `NULL` / `None` sentiment label.




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
