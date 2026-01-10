import pandas as pd
from typing import Dict, Optional

class ReconDataNormalizer:
    def __init__(self, df: pd.DataFrame):
        self.df: pd.DataFrame = df.copy()

    def normalize_columns(self) -> "ReconDataNormalizer":
        self.df.columns = self.df.columns.str.strip().str.lower()
        return self

    def sanitize_strings(self) -> "ReconDataNormalizer":
        string_cols = self.df.select_dtypes(include=["object", "string"]).columns

        for col in string_cols:
            s = (
                self.df[col]
                .astype(str)
                .str.strip()
                .str.lower()
                .str.replace(r"\s+", " ", regex=True)
                .str.replace(r"[\"\']", "", regex=True)
            )

            self.df[col] = s.replace(
                {"nan": pd.NA, "none": pd.NA, "": pd.NA}
            )

        return self

    def normalize_datetime(
        self,
        column: str,
        tz: Optional[str] = None
    ) -> "ReconDataNormalizer":

        if column not in self.df.columns:
            return self

        raw = (
            self.df[column]
            .astype(str)
            .str.strip()
        )

        parsed = pd.to_datetime(
            raw,
            errors="coerce",
            utc=bool(tz)
        )

        mask = parsed.isna()
        if mask.any():
            parsed.loc[mask] = pd.to_datetime(
                raw.loc[mask],
                errors="coerce",
                dayfirst=True,
                utc=bool(tz)
            )

        if tz:
            parsed = parsed.dt.tz_convert(tz)

        self.df[column] = parsed
        return self

    def clean_amount(self, column: str) -> "ReconDataNormalizer":
        if column not in self.df.columns:
            return self

        cleaned = (
            self.df[column]
            .astype(str)
            .str.replace(r"[₹$,]", "", regex=True)
            .str.replace(r"\.0+$", "", regex=True)
            .str.strip()
        )

        self.df[column] = pd.to_numeric(cleaned, errors="coerce")
        return self

    def enforce_schema(
        self,
        schema: Dict[str, str]
    ) -> "ReconDataNormalizer":

        for col, dtype in schema.items():
            if col not in self.df.columns:
                continue

            if dtype == "string":
                self.df[col] = self.df[col].astype("string")

            elif dtype == "float":
                self.df[col] = pd.to_numeric(self.df[col], errors="coerce")

            elif dtype == "int":
                self.df[col] = (
                    pd.to_numeric(self.df[col], errors="coerce")
                    .astype("Int64")
                )

            elif dtype == "datetime":
                self.df[col] = pd.to_datetime(self.df[col], errors="coerce")

            else:
                raise ValueError(f"Unsupported dtype: {dtype}")

        return self

    def run_all(
        self,
        datetime_column: Optional[str],
        amount_column: Optional[str],
        schema: Dict[str, str],
        tz: Optional[str] = None
    ) -> pd.DataFrame:

        (
            self.normalize_columns()
            .sanitize_strings()
            .normalize_datetime(datetime_column, tz)
            .clean_amount(amount_column)
            .enforce_schema(schema)
        )

        return self.df

    def get_df(self) -> pd.DataFrame:
        return self.df
