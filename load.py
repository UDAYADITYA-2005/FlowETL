"""
load.py - Load layer.
Validates the transformed DataFrame, then loads into SQLite with ACID guarantees.
Raises ValidationError on any failure — no partial loads.
"""
import sqlite3
import pandas as pd
from logger import get_logger
from config import DB_PATH, TARGET_TABLE, VALIDATION

log = get_logger("load")


class ValidationError(Exception):
    pass


def validate(df: pd.DataFrame) -> list[str]:
    """
    Run all validation checks. Returns list of error messages.
    Empty list means validation passed.
    """
    errors = []

    # Check 1: minimum row count
    if len(df) < VALIDATION["min_rows"]:
        errors.append(f"Row count {len(df)} is below minimum {VALIDATION['min_rows']}")

    # Check 2: required columns present
    missing = [c for c in VALIDATION["required_columns"] if c not in df.columns]
    if missing:
        errors.append(f"Missing required columns: {missing}")

    # Check 3: non-null columns
    for col in VALIDATION["non_null_columns"]:
        if col in df.columns and df[col].isnull().any():
            errors.append(f"Column '{col}' must not contain nulls")

    # Check 4: primary key uniqueness
    pk = VALIDATION["primary_key"]
    if pk in df.columns:
        dupes = df[pk].duplicated().sum()
        if dupes > 0:
            errors.append(f"Primary key '{pk}' has {dupes} duplicate values")

    # Check 5: no completely empty columns
    all_null_cols = [c for c in df.columns if df[c].isnull().all()]
    if all_null_cols:
        errors.append(f"Columns with all nulls: {all_null_cols}")

    return errors


def load(df: pd.DataFrame) -> dict:
    """
    Validate and load DataFrame into SQLite.
    Returns metadata dict about the load operation.
    """
    log.info("=" * 60)
    log.info("LOAD LAYER STARTED")
    log.info("=" * 60)

    # --- Validate ---
    log.info("Running validation checks...")
    errors = validate(df)
    if errors:
        for e in errors:
            log.error(f"VALIDATION FAILED: {e}")
        raise ValidationError(f"Load aborted. {len(errors)} validation error(s): {errors}")
    log.info(f"All {len(VALIDATION)} validation checks passed.")

    # --- Load with transaction ---
    log.info(f"Loading {len(df)} rows into [{TARGET_TABLE}] at {DB_PATH}")
    conn = sqlite3.connect(DB_PATH)
    try:
        conn.execute("BEGIN")

        # Drop and recreate for idempotency (re-running pipeline gives same result)
        conn.execute(f"DROP TABLE IF EXISTS {TARGET_TABLE}")

        df.to_sql(TARGET_TABLE, conn, if_exists="replace", index=False)

        # Post-load verification
        cursor = conn.execute(f"SELECT COUNT(*) FROM {TARGET_TABLE}")
        db_count = cursor.fetchone()[0]

        if db_count != len(df):
            raise ValidationError(
                f"Row count mismatch after load: DataFrame={len(df)}, DB={db_count}"
            )

        conn.commit()
        log.info(f"Load committed. {db_count} rows in [{TARGET_TABLE}].")

        # Create useful indexes
        conn.execute(f"CREATE INDEX IF NOT EXISTS idx_state ON {TARGET_TABLE}(state)")
        conn.execute(f"CREATE INDEX IF NOT EXISTS idx_devidx ON {TARGET_TABLE}(dev_index)")
        conn.commit()
        log.info("Indexes created on [state] and [dev_index].")

    except Exception as e:
        conn.rollback()
        log.error(f"Load failed. Transaction rolled back. Error: {e}")
        raise
    finally:
        conn.close()

    return {
        "rows_loaded": db_count,
        "table": TARGET_TABLE,
        "db_path": DB_PATH,
        "columns": list(df.columns),
    }
