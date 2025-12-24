
import pandas as pd
import logging
from reviews_cleaning import DataCleaner

logger = logging.getLogger(__name__)

def clean_training_dataset(
    input_path: str,
    output_path: str
):
    # --------------------
    # Load data
    # --------------------
    df = pd.read_csv(input_path)

    # Clean ratings
    df = df[df['rating']!='|']
    df['rating'] = df['rating'].astype('float64')

    # --------------------
    # Text cleaning
    # --------------------
    df = DataCleaner.remove_links_from_reviews(
        df, target_col="review_content"
        )
    df['review_content_en'] = df['review_content'].apply(DataCleaner.keep_english_sentences)

    def english_retention_ratio(original, cleaned):
        if not original:
            return 0
        return round(len(cleaned) / len(original), 2)

    df['retention_ratio'] = df.apply(
        lambda r: english_retention_ratio(r['review_content'], r['review_content_en']),
        axis=1
    )

    logger.info(
        "English retention stats: min=%s avg=%s max=%s",
        df['retention_ratio'].min(),
        df['retention_ratio'].mean(),
        df['retention_ratio'].max()
    )
    cols_to_clean = ["review_content_en", "review_title", "about_product"]

    for c in cols_to_clean:
        if c in df.columns:
            df[f"{c}_clean"] = DataCleaner.standardize_text(df[c])

    df = df.drop(columns=[c for c in cols_to_clean if c in df.columns])

    # Drop duplicates

    df = df.drop_duplicates(
        subset=["product_id", "user_id", "review_content_en_clean"],
        keep="first"
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


    # --------------------
    # Data quality checks
    # --------------------
    DataCleaner.enforce_null_thresholds(df)


    # --------------------
    # Guarded null row dropping
    # --------------------
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
    print("\nSentiment distribution:")
    print(label_dist)

    # --------------------
    # Output
    # --------------------
    df = df[["product_id", "review_content_en_clean", "sentiment"]]
    df.to_csv(
        f"{output_path}/clean_training_dataset.csv",
        index=False
    )
