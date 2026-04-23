# SmartIngest — Multi-Source CSV-to-DB ETL Pipeline

An end-to-end ETL pipeline in Python that ingests Indian Census data from 3 CSV sources,
applies Medallion-style transformations (Bronze → Silver → Gold), loads into SQLite,
and exposes results through an interactive Streamlit dashboard.

## Project Structure

```
etl_pipeline/
├── generate_data.py     # Seed 3 realistic CSV datasets
├── config.py            # Central config (paths, validation rules, state map)
├── logger.py            # Shared logger (console + file)
├── extract.py           # BRONZE: read CSVs, tag source, log nulls
├── transform.py         # SILVER: clean, standardize, merge, derive columns
├── load.py              # GOLD: validate, load to SQLite with ACID transaction
├── report.py            # Charts + text run summary
├── pipeline.py          # Orchestrator: runs Extract → Transform → Load → Report
├── dashboard.py         # Streamlit frontend
├── requirements.txt
├── queries/
│   └── analytics.sql    # 10 analytical SQL queries (window fns, CTEs, CASE WHEN)
├── data/
│   ├── raw/             # Source CSV files (generated)
│   └── processed/       # SQLite database
├── reports/             # Per-run text reports + chart PNGs
└── logs/                # Per-run .log files
```

## Quick Start

```bash
# 1. Install dependencies
pip install -r requirements.txt

# 2. Generate sample datasets
python generate_data.py

# 3. Run the full ETL pipeline
python pipeline.py

# 4. Launch the dashboard
streamlit run dashboard.py
```

## Pipeline Layers

| Layer | Module | What happens |
|---|---|---|
| Bronze | `extract.py` | Reads 3 CSVs as-is, adds `_source` and `_extracted_at` tags |
| Silver | `transform.py` | Standardizes columns, cleans state names, imputes nulls, deduplicates, merges, derives 5 new columns |
| Gold | `load.py` | Validates (5 checks), loads into SQLite atomically, creates indexes |

## Key Features

- Modular Extract / Transform / Load architecture
- Automated data quality checks with rollback on failure
- State name standardization (UP → Uttar Pradesh etc.)
- Null imputation (median for numeric, Unknown for categorical)
- 5 derived analytical columns including a composite development index
- Per-run structured logging to file and console
- 4 Matplotlib charts auto-generated per run
- Streamlit dashboard with SQL Workbench, Data Explorer, and Report Viewer

## Data Sources

All datasets generated via `generate_data.py` using realistic Indian census parameters.
Production equivalent: data.gov.in (OGDL v1.0 license — free for personal/educational use).

## License

MIT License. Original code by Gali Uday Aditya.
Data generated synthetically for educational use.
