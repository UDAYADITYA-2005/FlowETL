"""
transform.py - Transform layer.
Cleans, standardizes, merges, and derives columns from the 3 raw DataFrames.
Produces a single analytics-ready DataFrame.
"""
import pandas as pd
import numpy as np
from logger import get_logger
from config import STATE_NAME_MAP

log = get_logger("transform")


# ── Quality report tracker ────────────────────────────────────────────────────
class QualityReport:
    def __init__(self):
        self.steps = []

    def record(self, step: str, detail: str):
        msg = f"[QC] {step}: {detail}"
        self.steps.append(msg)
        log.debug(msg)

    def summary(self) -> str:
        return "\n".join(self.steps)


qr = QualityReport()


# ── Helpers ───────────────────────────────────────────────────────────────────
def _standardize_columns(df: pd.DataFrame, source: str) -> pd.DataFrame:
    """Lowercase all column names and replace spaces with underscores."""
    before = list(df.columns)
    df.columns = [c.strip().lower().replace(" ", "_").replace("(", "").replace(")", "").replace("%","pct").replace("/","_") for c in df.columns]
    after = list(df.columns)
    qr.record(f"{source}:columns", f"standardised {len(before)} column names")
    log.debug(f"[{source}] columns: {before} -> {after}")
    return df


def _clean_state_names(df: pd.DataFrame, state_col: str, source: str) -> pd.DataFrame:
    """Standardise dirty state name variants to canonical names."""
    original = df[state_col].copy()
    df[state_col] = (
        df[state_col]
        .str.strip()
        .str.lower()
        .map(lambda x: STATE_NAME_MAP.get(x, x.title()) if isinstance(x, str) else x)
    )
    changed = (original != df[state_col]).sum()
    qr.record(f"{source}:state_names", f"standardised {changed} dirty state name variants")
    return df


def _cast_numeric(df: pd.DataFrame, cols: list, source: str) -> pd.DataFrame:
    """Cast specified columns to numeric, coercing errors to NaN."""
    for col in cols:
        if col in df.columns:
            before_nulls = df[col].isnull().sum()
            df[col] = pd.to_numeric(df[col], errors="coerce")
            after_nulls = df[col].isnull().sum()
            new_nulls = after_nulls - before_nulls
            if new_nulls > 0:
                qr.record(f"{source}:cast:{col}", f"{new_nulls} values coerced to NaN (non-numeric)")
    return df


def _impute_nulls(df: pd.DataFrame, source: str) -> pd.DataFrame:
    """Fill nulls: median for numeric columns, 'Unknown' for categorical."""
    null_before = df.isnull().sum().sum()
    for col in df.select_dtypes(include=[np.number]).columns:
        n = df[col].isnull().sum()
        if n > 0:
            median_val = df[col].median()
            df[col] = df[col].fillna(median_val)
            qr.record(f"{source}:impute:{col}", f"filled {n} nulls with median={median_val:.2f}")
    for col in df.select_dtypes(include=["object"]).columns:
        n = df[col].isnull().sum()
        if n > 0 and col not in ("_source", "_extracted_at"):
            df[col] = df[col].fillna("Unknown")
            qr.record(f"{source}:impute:{col}", f"filled {n} nulls with 'Unknown'")
    null_after = df.isnull().sum().sum()
    qr.record(f"{source}:impute_total", f"nulls reduced from {null_before} to {null_after}")
    return df


def _drop_duplicates(df: pd.DataFrame, key: str, source: str) -> pd.DataFrame:
    before = len(df)
    df = df.drop_duplicates(subset=[key], keep="first")
    dropped = before - len(df)
    qr.record(f"{source}:dedup", f"removed {dropped} duplicate rows on key='{key}'")
    return df


# ── Per-source transforms ─────────────────────────────────────────────────────
def transform_population(df: pd.DataFrame) -> pd.DataFrame:
    log.info("Transforming: population")
    df = _standardize_columns(df, "population")
    df = df.rename(columns={"state_name": "state"})
    df = _clean_state_names(df, "state", "population")
    numeric_cols = ["total_population","male_population","female_population",
                    "area_sq_km","population_density","urban_population_pct","year"]
    df = _cast_numeric(df, numeric_cols, "population")
    df["rural_population_pct"] = 100 - df["urban_population_pct"]
    qr.record("population:derive", "derived rural_population_pct = 100 - urban_population_pct")
    df = _impute_nulls(df, "population")
    df = _drop_duplicates(df, "state", "population")
    internal = [c for c in df.columns if c.startswith("_")]
    df = df.drop(columns=internal)
    return df


def transform_literacy(df: pd.DataFrame) -> pd.DataFrame:
    log.info("Transforming: literacy")
    df = _standardize_columns(df, "literacy")
    df = _clean_state_names(df, "state", "literacy")
    numeric_cols = ["literacy_rate_overall","literacy_rate_male","literacy_rate_female",
                    "primary_schools_per_1000","higher_education_institutions",
                    "govt_schools_pct","survey_year"]
    df = _cast_numeric(df, numeric_cols, "literacy")
    df["gender_literacy_gap"] = df["literacy_rate_male"] - df["literacy_rate_female"]
    qr.record("literacy:derive", "derived gender_literacy_gap = male_rate - female_rate")
    df = _impute_nulls(df, "literacy")
    df = _drop_duplicates(df, "state", "literacy")
    internal = [c for c in df.columns if c.startswith("_")]
    df = df.drop(columns=internal)
    return df


def transform_employment(df: pd.DataFrame) -> pd.DataFrame:
    log.info("Transforming: employment")
    df = _standardize_columns(df, "employment")
    df = df.rename(columns={"state": "state"})
    state_col = [c for c in df.columns if "state" in c][0]
    df = df.rename(columns={state_col: "state"})
    df = _clean_state_names(df, "state", "employment")
    numeric_cols = ["labour_force_participation_pct","unemployment_rate_pct",
                    "agriculture_employment_pct","services_employment_pct",
                    "manufacturing_employment_pct","per_capita_income_inr",
                    "gdp_contribution_pct","year"]
    df = _cast_numeric(df, numeric_cols, "employment")
    df = _impute_nulls(df, "employment")
    df = _drop_duplicates(df, "state", "employment")
    internal = [c for c in df.columns if c.startswith("_")]
    df = df.drop(columns=internal)
    return df


# ── Merge & derive final columns ──────────────────────────────────────────────
def merge_and_derive(pop: pd.DataFrame, lit: pd.DataFrame, emp: pd.DataFrame) -> pd.DataFrame:
    log.info("Merging all 3 DataFrames on 'state'")
    df = pop.merge(lit, on="state", how="inner")
    log.info(f"After pop+lit merge: {len(df)} rows")
    df = df.merge(emp, on="state", how="inner")
    log.info(f"After full merge: {len(df)} rows")

    # Derived column 1: literacy-employment ratio
    df["literacy_employment_ratio"] = (
        df["literacy_rate_overall"] / df["labour_force_participation_pct"]
    ).round(4)
    qr.record("merge:derive", "derived literacy_employment_ratio")

    # Derived column 2: population per school
    df["population_per_school"] = (
        df["total_population"] / (df["primary_schools_per_1000"] * df["total_population"] / 1000)
    ).round(2)
    qr.record("merge:derive", "derived population_per_school")

    # Derived column 3: economic development index (simple composite)
    df["dev_index"] = (
        (df["literacy_rate_overall"] / 100) * 0.4
        + (1 - df["unemployment_rate_pct"] / 100) * 0.3
        + (df["per_capita_income_inr"] / df["per_capita_income_inr"].max()) * 0.3
    ).round(4)
    qr.record("merge:derive", "derived dev_index (composite: literacy + employment + income)")

    df["processed_at"] = pd.Timestamp.now().isoformat()
    log.info(f"Final merged DataFrame: {len(df)} rows x {len(df.columns)} columns")
    return df


# ── Main transform entry point ────────────────────────────────────────────────
def transform_all(frames: dict) -> tuple[pd.DataFrame, QualityReport]:
    log.info("=" * 60)
    log.info("TRANSFORM LAYER STARTED")
    log.info("=" * 60)

    pop = transform_population(frames["population"].copy())
    lit = transform_literacy(frames["literacy"].copy())
    emp = transform_employment(frames["employment"].copy())

    df_final = merge_and_derive(pop, lit, emp)

    # Final null check
    remaining_nulls = df_final.isnull().sum().sum()
    qr.record("final_check", f"remaining nulls after all transforms: {remaining_nulls}")

    log.info("Transform layer complete.")
    log.info(f"\n{qr.summary()}")
    return df_final, qr
