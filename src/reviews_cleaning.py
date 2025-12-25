import pandas as pd
import numpy as np
import logging
import re

from langdetect import detect, LangDetectException

logger = logging.getLogger(__name__)


class DataCleaner:

    @staticmethod
    def clean_currency(series: pd.Series) -> pd.Series:
        return (
            series.astype(str)
            .str.replace(r"[^\d\.,-]", "", regex=True)
            .str.replace(",", "", regex=False)
            .astype(float)
        )

    @staticmethod
    def clean_percentage(series: pd.Series) -> pd.Series:
        return series.astype(str).str.replace(r"[^\d\.-]", "", regex=True).astype(float)

    @staticmethod
    def keep_english_sentences(text):
        if not isinstance(text, str):
            return text

        sentences = re.split(r"[.!?,]", text)
        english_sentences = []

        for s in sentences:
            s = s.strip()
            if len(s) < 5:
                continue

            try:
                lang = detect(s)
                if lang == "en":
                    english_sentences.append(s)
            except LangDetectException:
                # Fallback: If it still fails, it's likely English with weird formatting.
                # We keep it for now.
                logger.warning("Language detection failed for sentence: %s", s)
                english_sentences.append(s)
                # continue

        return ". ".join(english_sentences)

    @staticmethod
    def standardize_text(series):
        if not isinstance(series, pd.Series):
            raise TypeError(
                "standardize_text expects a pandas Series, " f"got {type(series)}"
            )

        return (
            series.astype(str)
            .str.lower()
            .str.replace(r"<[^>]+>", " ", regex=True)
            .str.replace(r"http\S+|www\S+", " ", regex=True)
            .str.replace(r"[^a-z0-9\s]", " ", regex=True)
            .str.replace(r"\s+", " ", regex=True)
            .str.strip()
        )

    @staticmethod
    def log_duplicate_ratio(df: pd.DataFrame, subset_cols: list) -> None:
        """
        Calculates and logs duplication stats.
        Returns None.
        """
        total_records = len(df)

        if total_records == 0:
            logging.info("Duplicate check skipped: DataFrame is empty.")
            return

        # Identify all rows that have at least one duplicate
        duplicate_records = df.duplicated(subset=subset_cols, keep=False).sum()
        duplicate_pct = round((duplicate_records / total_records) * 100, 2)

        # Format column names for the log table
        cols_display = (
            ", ".join(subset_cols)
            if isinstance(subset_cols, list)
            else str(subset_cols)
        )

        # Prepare Stats Table
        stats_df = pd.DataFrame(
            {
                "Metric": [
                    "Total Records",
                    "Duplicate Records",
                    "Duplicate Percentage",
                    "Subset Columns",
                ],
                "Value": [
                    total_records,
                    duplicate_records,
                    f"{duplicate_pct}%",
                    cols_display,
                ],
            }
        )

        # Log structured report
        log_message = (
            "\n" + "=" * 45 + "\n"
            "DATASET DUPLICATION REPORT\n"
            + "=" * 45
            + "\n"
            + stats_df.to_string(index=False)
            + "\n"
            + "=" * 45
        )
        logging.info(log_message)

    @staticmethod
    def add_sentiment_column(
        df: pd.DataFrame, rating_col="rating", sentiment_col="sentiment"
    ) -> pd.DataFrame:

        df = df.copy()
        df[rating_col] = pd.to_numeric(df[rating_col], errors="coerce")

        df[sentiment_col] = np.select(
            [
                (df[rating_col] >= 1.0) & (df[rating_col] < 3.0),
                (df[rating_col] >= 3.0) & (df[rating_col] < 4.0),
                (df[rating_col] >= 4.0) & (df[rating_col] <= 5.0),
            ],
            ["negative", "neutral", "positive"],
            default=None,
        )

        return df

    @staticmethod
    def label_distribution(df: pd.DataFrame, label_col="sentiment") -> pd.DataFrame:

        if df.empty:
            return pd.DataFrame(columns=[label_col, "count", "percentage"])

        dist = (
            df["sentiment"]
            .value_counts(dropna=False)
            .rename("count")
            .reset_index()
            .rename(columns={"index": "sentiment"})
        )
        dist["percentage"] = round(dist["count"] / dist["count"].sum() * 100, 2)

        return dist

    @staticmethod
    def dropnull_with_guard(
        df: pd.DataFrame, max_row_null_pct=40.0, max_total_loss_pct=40.0
    ) -> pd.DataFrame:
        """
        1. Blocks if any single row is too empty.
        2. Blocks if the total amount of data being dropped is too high.
        """
        total_rows_before = len(df)
        total_cols = len(df.columns)

        # --- Individual Row Quality ---
        row_null_pct = (df.isna().sum(axis=1) / total_cols) * 100
        violating_rows = df[row_null_pct > max_row_null_pct]

        if not violating_rows.empty:
            sample = row_null_pct[row_null_pct > max_row_null_pct].head(5)
            raise ValueError(
                f"DATA INTEGRITY ERROR: {len(violating_rows)} rows are too 'holey' "
                f"(>{max_row_null_pct}% null). Investigate source data. \n"
                f"Sample null percentages:\n{sample}"
            )

        # ---  Total Dataset Volume ---
        df_clean = df.dropna()
        total_rows_after = len(df_clean)

        loss_pct = ((total_rows_before - total_rows_after) / total_rows_before) * 100

        if loss_pct > max_total_loss_pct:
            raise RuntimeError(
                f"DATA LOSS GUARD TRIGGERED: Dropping nulls would remove {loss_pct:.2f}% "
                f"of your data, which exceeds the {max_total_loss_pct}% limit."
            )

        return df_clean

    @staticmethod
    def remove_links_from_reviews(
        df: pd.DataFrame,
        target_col: str = "review_content",
    ) -> pd.DataFrame:
        """
        Detects and removes URLs from review text.
        Logs structured stats to Airflow logs.
        """

        if df.empty:
            logging.info("Link removal skipped: empty DataFrame")
            return df

        url_pattern = re.compile(r"http[s]?://\S+|www\.\S+")

        # 1. Detect reviews with links
        has_link = (
            df[target_col].astype(str).str.contains(url_pattern, regex=True, na=False)
        )

        total_reviews = len(df)
        reviews_with_links = has_link.sum()
        pct = round((reviews_with_links / total_reviews) * 100, 2)

        # Format Structured Log Output
        stats_data = {
            "Metric": ["Total Reviews", "Reviews with Links", "Percentage"],
            "Value": [total_reviews, reviews_with_links, f"{pct}%"],
        }
        stats_df = pd.DataFrame(stats_data)

        log_message = (
            "\n" + "=" * 35 + "\n"
            "LINK REMOVAL REPORT\n"
            + "=" * 35
            + "\n"
            + stats_df.to_string(index=False)
            + "\n"
            + "=" * 35
        )
        logging.info(log_message)

        # Remove links and normalize whitespace
        df[target_col] = (
            df[target_col]
            .astype(str)
            .str.replace(url_pattern, " ", regex=True)
            .str.replace(r"\s+", " ", regex=True)
            .str.strip()
        )

        return df
