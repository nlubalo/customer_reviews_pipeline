import pandas as pd
import logging
from reviews_cleaning import DataCleaner

logger = logging.getLogger(__name__)


def clean_training_dataset(input_path: str, output_path: str):
    # --------------------
    # Load data
    # --------------------
    df = pd.read_csv(input_path)

    # Clean ratings
    df = df[df["rating"] != "|"]
    df["rating"] = df["rating"].astype("float64")

    # --------------------
    # Text cleaning
    # --------------------

    # 1. Remove links from reviews
    df = DataCleaner.remove_links_from_reviews(df, target_col="review_content")
    # 2. Keep only English sentences and standardize text
    df["review_content_en"] = df["review_content"].apply(
        DataCleaner.keep_english_sentences
    )

    def english_retention_ratio(original, cleaned):
        if not original:
            return 0
        return round(len(cleaned) / len(original), 2)

    df["retention_ratio"] = df.apply(
        lambda r: english_retention_ratio(r["review_content"], r["review_content_en"]),
        axis=1,
    )

    stats = {
        "Metric": ["Min Ratio", "Avg Ratio", "Max Ratio"],
        "Value": [
            f"{df['retention_ratio'].min():.2f}",
            f"{df['retention_ratio'].mean():.2f}",
            f"{df['retention_ratio'].max():.2f}",
        ],
    }
    stats_df = pd.DataFrame(stats)

    log_message = (
        "\n" + "=" * 35 + "\n"
        "ENGLISH RETENTION STATS\n"
        + "=" * 35
        + "\n"
        + stats_df.to_string(index=False)
        + "\n"
        + "=" * 35
    )

    logging.info(log_message)

    # 3. Standardize text in relevant columns
    cols_to_clean = ["review_content_en", "review_title", "about_product"]

    for c in cols_to_clean:
        if c in df.columns:
            df[f"{c}_clean"] = DataCleaner.standardize_text(df[c])

    df = df.drop(columns=[c for c in cols_to_clean if c in df.columns])

    # Drop duplicates
    DataCleaner.log_duplicate_ratio(
        df, subset_cols=["product_id", "user_id", "review_content_en_clean"]
    )
    df = df.drop_duplicates(
        subset=["product_id", "user_id", "review_content_en_clean"], keep="first"
    )

    # --------------------
    # Currency and percentage cleaning
    # --------------------

    cols_to_clean_currency = ["actual_price", "discounted_price"]
    for c in cols_to_clean_currency:
        if c in df.columns:
            df[f"{c}_clean"] = DataCleaner.clean_currency(df[c])
            df = df.drop(columns=[c])

    cols_to_clean_percentage = ["discount_percentage"]
    for c in cols_to_clean_percentage:
        if c in df.columns:
            df[f"{c}_clean"] = DataCleaner.clean_percentage(df[c])
            df = df.drop(columns=[c])

    df = DataCleaner.dropnull_with_guard(
        df,
    )

    # --------------------
    # Sentiment labeling
    # --------------------
    df = DataCleaner.add_sentiment_column(df)

    # --------------------
    # Label distribution (logged, not enforced)
    # --------------------
    label_dist = DataCleaner.label_distribution(df)
    log_message = (
        "\n" + "=" * 30 + "\n"
        "SENTIMENT DISTRIBUTION REPORT\n"
        + "=" * 30
        + "\n"
        + label_dist.to_string(index=False)
        + "\n"
        + "=" * 30
    )
    logging.info(log_message)

    # --------------------
    # Output
    # --------------------
    df = df[["product_id", "review_content_en_clean", "sentiment"]]
    df.to_csv(f"{output_path}/clean_training_dataset.csv", index=False)
