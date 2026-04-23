"""
config.py - Central configuration for the ETL pipeline.
Change paths and settings here without touching pipeline code.
"""
import os
from datetime import datetime

# ── Paths ────────────────────────────────────────────────────────────────────
BASE_DIR       = os.path.dirname(os.path.abspath(__file__))
DATA_RAW_DIR   = os.path.join(BASE_DIR, "data", "raw")
DATA_PROC_DIR  = os.path.join(BASE_DIR, "data", "processed")
REPORTS_DIR    = os.path.join(BASE_DIR, "reports")
LOGS_DIR       = os.path.join(BASE_DIR, "logs")
QUERIES_DIR    = os.path.join(BASE_DIR, "queries")

DB_PATH        = os.path.join(DATA_PROC_DIR, "india_census.db")

# ── Source file names ────────────────────────────────────────────────────────
SOURCE_FILES = {
    "population":  os.path.join(DATA_RAW_DIR, "population.csv"),
    "literacy":    os.path.join(DATA_RAW_DIR, "literacy.csv"),
    "employment":  os.path.join(DATA_RAW_DIR, "employment.csv"),
}

# ── Target table name ────────────────────────────────────────────────────────
TARGET_TABLE = "india_state_census"

# ── Validation rules ─────────────────────────────────────────────────────────
VALIDATION = {
    "min_rows": 10,
    "required_columns": [
        "state", "total_population", "literacy_rate_overall",
        "unemployment_rate_pct", "per_capita_income_inr",
    ],
    "non_null_columns": ["state"],
    "primary_key": "state",
}

# ── Run metadata ─────────────────────────────────────────────────────────────
RUN_ID        = datetime.now().strftime("%Y%m%d_%H%M%S")
RUN_DATE      = datetime.now().strftime("%Y-%m-%d")
REPORT_FILE   = os.path.join(REPORTS_DIR, f"run_{RUN_ID}.txt")
LOG_FILE      = os.path.join(LOGS_DIR,    f"run_{RUN_ID}.log")

# ── Canonical state name map (dirty → clean) ─────────────────────────────────
STATE_NAME_MAP = {
    "up": "Uttar Pradesh", "u.p.": "Uttar Pradesh", "uttar pradesh": "Uttar Pradesh",
    "mp": "Madhya Pradesh", "m.p.": "Madhya Pradesh", "madhya pradesh": "Madhya Pradesh",
    "wb": "West Bengal", "w.b.": "West Bengal", "west bengal": "West Bengal",
    "tn": "Tamil Nadu", "t.n.": "Tamil Nadu", "tamil nadu": "Tamil Nadu",
    "ap": "Andhra Pradesh", "a.p.": "Andhra Pradesh", "andhra pradesh": "Andhra Pradesh",
}

for d in [DATA_RAW_DIR, DATA_PROC_DIR, REPORTS_DIR, LOGS_DIR, QUERIES_DIR]:
    os.makedirs(d, exist_ok=True)