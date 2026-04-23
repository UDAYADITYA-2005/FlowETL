### FlowETL: Modular Batch Processing Pipeline

**Project Overview**
FlowETL is a traditional, production-ready data engineering pipeline designed to handle the core **Extract, Transform, Load** lifecycle. Built for scalability and modularity, the project automates the movement of data from various raw formats into a structured, analytics-ready environment, supported by a custom monitoring dashboard and robust logging.

**Architecture Flowchart**
```text
┌─────────────────────────────────────────────────────────────┐
│                       DATA SOURCES                          │
│           CSV Files  |  JSON Files  |  REST APIs            │
└──────────────┬─────────────────┬───────────────┬────────────┘
               │                 │               │
               ▼                 ▼               ▼
┌─────────────────────────────────────────────────────────────┐
│                      📥 EXTRACT LAYER                       │
│       Multi-source Ingestion → Raw Data Staging             │
└──────────────────────────────┬──────────────────────────────┘
                               │
                               ▼
┌─────────────────────────────────────────────────────────────┐
│                    ⚙️ TRANSFORM LAYER                       │
│   Data Cleansing | Type Casting | Business Logic Application│
│        Standardization & Handling of Missing Values         │
└──────────────────────────────┬──────────────────────────────┘
                               │
                               ▼
┌─────────────────────────────────────────────────────────────┐
│                        📤 LOAD LAYER                        │
│     Final Data Consolidation → Parquet & CSV Exports        │
│          Preparation for Downstream Analytics               │
└───────────────────────────┬────────────┬────────────────────┘
                            │            │
                            ▼            ▼
            ┌────────────────────────────────┐
            │   📊 Monitoring & Delivery     │
            │   • Custom HTML Dashboard      │
            │   • Structured Log Reports     │
            │   • Pipeline Status Metrics    │
            └────────────────────────────────┘
```

**Detailed Pipeline Stages**
The system is architected into specific modules to ensure a clean separation of concerns:

1.  **Extraction (`extract.py`):** The entry point of the pipeline. It handles the connections to various data sources, including local file systems and external APIs, fetching raw data into a centralized staging area.
2.  **Transformation (`transform.py`):** The processing engine. This module applies data-cleansing rules, enforces data types, and executes the necessary business logic to convert raw records into a consistent format.
3.  **Loading (`load.py`):** The final stage of the ETL flow. It handles the efficient storage of processed data into high-performance formats like Apache Parquet and accessible CSVs for final reporting.
4.  **Monitoring & Reporting (`report.py` & `dashboard.py`):** A dedicated layer that provides visibility into the pipeline's performance. It tracks record counts, execution times, and success rates, serving them through a live dashboard.
5.  **Orchestration (`pipeline.py`):** The main controller that manages the sequence of operations, ensuring that each layer executes only upon the successful completion of the previous stage.

**How to Run the Project locally**

**1. Clone the repository**
```bash
git clone https://github.com/UDAYADITYA-2005/etl_pipeline.git
cd etl_pipeline
```

**2. Create and activate a virtual environment**
* **Windows:**
    ```bash
    python -m venv venv
    venv\Scripts\activate
    ```
* **Mac / Linux:**
    ```bash
    python3 -m venv venv
    source venv/bin/activate
    ```

**3. Install dependencies**
```bash
pip install -r requirements.txt
```

**4. Generate sample data & run the pipeline**
```bash
# Optional: Generate synthetic data for testing
python generate_data.py

# Run the full ETL process
python pipeline.py
```

**5. View the dashboard**
```bash
python dashboard.py
```
Once the dashboard script is running, open your web browser to the local address provided in the terminal to see your processed data metrics.
