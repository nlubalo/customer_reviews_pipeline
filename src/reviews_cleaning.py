
import pandas as pd
import numpy as np
import logging
import re

from langdetect import detect, LangDetectException

logger = logging.getLogger(__name__)

class DataCleaner:

    @staticmethod
    def enforce_null_thresholds(
        df: pd.DataFrame,
        max_column_null_pct=20.0,
        max_row_null_pct=10.0
    ):
        """
        Fails the pipeline if null thresholds are exceeded.
        """

        if df.empty:
            return {"status": "PASSED", "reason": "Empty DataFrame"}

        total_rows = len(df)
        total_cols = len(df.columns)

        # ---------- Column-level nulls ----------
        col_null_pct = (df.isna().sum() / total_rows) * 100
        columns_breaching = col_null_pct[col_null_pct > max_column_null_pct]

        # ---------- Row-level nulls ----------
        row_null_pct = (df.isna().sum(axis=1) / total_cols) * 100
        rows_with_nulls_pct = round((row_null_pct > 0).mean() * 100, 2)

        errors = []

        if not columns_breaching.empty:
            errors.append(
                f"Columns exceeding {max_column_null_pct}% nulls: "
                + ", ".join(
                    f"{c} ({round(pct, 2)}%)"
                    for c, pct in columns_breaching.items()
                )
            )

        if rows_with_nulls_pct > max_row_null_pct:
            errors.append(
                f"Rows with nulls {rows_with_nulls_pct}% exceed allowed {max_row_null_pct}%"
            )

        if errors:
            raise ValueError("DATA QUALITY CHECK FAILED\n" + "\n".join(errors))

        return {
            "status": "PASSED",
            "row_null_percentage": rows_with_nulls_pct,
            "columns_checked": total_cols
        }

    # ---------------------------------------------------
    # Currency / percentage cleaning
    # ---------------------------------------------------
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
        return (
            series.astype(str)
                  .str.replace(r"[^\d\.-]", "", regex=True)
                  .astype(float)
        )

    # ---------------------------------------------------
    # Text standardization
    # ---------------------------------------------------
    @staticmethod
    def keep_english_sentences(text):
        if not isinstance(text, str):
            return text

        sentences = re.split(r'[.!?,]', text)
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
                logger.warning("Language detection failed for sentence: %s", s)
                continue

        return ". ".join(english_sentences)
    @staticmethod
    def standardize_text(series):
        if not isinstance(series, pd.Series):
            raise TypeError(
                "standardize_text expects a pandas Series, "
                f"got {type(series)}"
            )

        return (
            series
            .astype(str)
            .str.lower()
            .str.replace(r"<[^>]+>", " ", regex=True)
            .str.replace(r"http\S+|www\S+", " ", regex=True)
            .str.replace(r"[^a-z0-9\s]", " ", regex=True)
            .str.replace(r"\s+", " ", regex=True)
            .str.strip()
        )

    # ---------------------------------------------------
    # Deduplication
    # ---------------------------------------------------
    @staticmethod
    def deduplicate_reviews(
        df: pd.DataFrame,
        subset_cols
    ) -> pd.DataFrame:
        return df.drop_duplicates(subset=subset_cols)

    @staticmethod
    def duplicate_ratio(
        df: pd.DataFrame,
        subset_cols
    ):
        total_records = len(df)

        if total_records == 0:
            return {
                "total_records": 0,
                "duplicate_records": 0,
                "duplicate_percentage": 0.0
            }

        duplicate_records = (
            df.duplicated(subset=subset_cols, keep=False).sum()
        )

        return {
            "total_records": total_records,
            "duplicate_records": duplicate_records,
            "duplicate_percentage": round(
                (duplicate_records / total_records) * 100, 2
            )
        }

    # ---------------------------------------------------
    # Sentiment labeling
    # ---------------------------------------------------
    @staticmethod
    def add_sentiment_column(
        df: pd.DataFrame,
        rating_col="rating",
        sentiment_col="sentiment"
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
            default=None
        )

        return df

    # ---------------------------------------------------
    # Label distribution (no failure)
    # ---------------------------------------------------
    @staticmethod
    def label_distribution(
        df: pd.DataFrame,
        label_col="sentiment"
    ) -> pd.DataFrame:
        if df.empty:
            return pd.DataFrame(
                columns=[label_col, "count", "percentage"]
            )

        dist = (
            df["sentiment"]
            .value_counts(dropna=False)
            .rename("count")
            .reset_index()
            .rename(columns={"index": "sentiment"})
        )
        dist["percentage"] = round(
            dist["count"] / dist["count"].sum() * 100,
            2
        )

        return dist

    # ---------------------------------------------------
    # Deduplication with null guard
    # ---------------------------------------------------
    @staticmethod
    def dropnull_with_guard(
        df: pd.DataFrame,
        max_row_null_pct=20.0
    ) -> pd.DataFrame:

        total_cols = len(df.columns)

        row_null_pct = (df.isna().sum(axis=1) / total_cols) * 100
        violating_rows = df[row_null_pct > max_row_null_pct]

        if not violating_rows.empty:
            sample = row_null_pct[row_null_pct > max_row_null_pct].head(5)

            raise ValueError(
                f"""
                DEDUPLICATION BLOCKED
                {len(violating_rows)} rows exceed allowed null ratio of {max_row_null_pct}%.
                Sample null percentages:
                {sample}
                """
            )

        return  df.dropna()

    @staticmethod
    def product_link_uniqueness_check(
        df: pd.DataFrame,
        product_link_col: str = "product_link",
        sample_size: int = 5
    ):
        """
        Logs whether product links are unique.

        Returns:
        - dict with total links, duplicate count, duplicate percentage
        """

        if df.empty:
            stats = {
                "total_links": 0,
                "duplicate_links": 0,
                "duplicate_percentage": 0.0
            }
            logger.info(
                "Product link uniqueness check: dataset is empty"
            )
            return stats

        # Only consider non-null links
        non_null_links = df[product_link_col].dropna()

        total_links = len(non_null_links)

        duplicated_mask = non_null_links.duplicated(keep=False)
        duplicate_links = duplicated_mask.sum()

        duplicate_percentage = round(
            (duplicate_links / total_links) * 100,
            2
        ) if total_links > 0 else 0.0

        stats = {
            "total_links": int(total_links),
            "duplicate_links": int(duplicate_links),
            "duplicate_percentage": duplicate_percentage
        }
        logger.info(
            "Product link uniqueness check | "
            "total_links=%s duplicate_links=%s duplicate_percentage=%s%%",
            stats["total_links"],
            stats["duplicate_links"],
            stats["duplicate_percentage"],
        )

        if duplicate_links > 0:
            sample = (
                non_null_links[duplicated_mask]
                .value_counts()
                .head(sample_size)
                .to_dict()
            )
            logger.warning(
                "Duplicate product links detected (sample=%s): %s",
                sample_size,
                sample
            )

        return stats

    @staticmethod
    def remove_links_from_reviews(
        df: pd.DataFrame,
        target_col: str = "review_content",
    ) -> pd.DataFrame:
        """
        Detects and removes URLs from review text.
        Logs stats but does NOT fail the pipeline.

        Parameters:
        - df: pandas DataFrame
        - review_col: column containing review text
        - cleaned_col: optional output column name (in-place if None)

        Returns:
        - DataFrame with links removed
        """

        if df.empty:
            logger.info("Link removal skipped: empty DataFrame")
            return df


        url_pattern = re.compile(r"http[s]?://\S+|www\.\S+")

        # Detect reviews with links
        has_link = df[target_col].astype(str).str.contains(
            url_pattern,
            regex=True,
            na=False
        )

        total_reviews = len(df)
        reviews_with_links = has_link.sum()
        pct = round((reviews_with_links / total_reviews) * 100, 2)

        logger.info(
            "Review link check | total_reviews=%s reviews_with_links=%s percentage=%s%%",
            total_reviews,
            reviews_with_links,
            pct
        )

        # Remove links
        df[target_col] = (
            df[target_col]
            .astype(str)
            .str.replace(url_pattern, " ", regex=True)
            .str.replace(r"\s+", " ", regex=True)
            .str.strip()
        )

        return df


