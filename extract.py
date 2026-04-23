"""
extract.py - Extract layer.
Each function reads one CSV source, tags it with metadata, and returns a raw DataFrame.
No transformation happens here — raw data is preserved exactly as-is.
"""
import pandas as pd
from datetime import datetime
from logger import get_logger
from config import SOURCE_FILES

log = get_logger("extract")


def _read_csv(path: str, source_name: str) -> pd.DataFrame:
    """Generic CSV reader with error handling and metadata tagging."""
    log.info(f"Reading source [{source_name}] from: {path}")
    try:
        df = pd.read_csv(path, dtype=str)   # read everything as str — transform layer casts types
        df["_source"]       = source_name
        df["_extracted_at"] = datetime.now().isoformat()
        log.info(f"[{source_name}] extracted {len(df)} rows, {len(df.columns)} columns")
        log.debug(f"[{source_name}] columns: {list(df.columns)}")

        null_counts = df.isnull().sum()
        nulls = null_counts[null_counts > 0]
        if not nulls.empty:
            log.warning(f"[{source_name}] nulls detected at extraction: {nulls.to_dict()}")
        return df
    except FileNotFoundError:
        log.error(f"[{source_name}] File not found: {path}. Run generate_data.py first.")
        raise
    except Exception as e:
        log.error(f"[{source_name}] Unexpected error during extraction: {e}")
        raise


def extract_population() -> pd.DataFrame:
    return _read_csv(SOURCE_FILES["population"], "population")


def extract_literacy() -> pd.DataFrame:
    return _read_csv(SOURCE_FILES["literacy"], "literacy")


def extract_employment() -> pd.DataFrame:
    return _read_csv(SOURCE_FILES["employment"], "employment")


def extract_all() -> dict[str, pd.DataFrame]:
    """Run all three extractions and return a dict of raw DataFrames."""
    log.info("=" * 60)
    log.info("EXTRACT LAYER STARTED")
    log.info("=" * 60)
    frames = {
        "population":  extract_population(),
        "literacy":    extract_literacy(),
        "employment":  extract_employment(),
    }
    total = sum(len(v) for v in frames.values())
    log.info(f"Extraction complete. Total rows across all sources: {total}")
    return frames
