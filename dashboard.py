"""
dashboard.py - Streamlit frontend for the ETL pipeline.
Run: streamlit run dashboard.py
"""
import os, glob, sqlite3, subprocess, sys
import pandas as pd
import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt
import matplotlib.gridspec as gridspec
import streamlit as st
from config import DB_PATH, TARGET_TABLE, REPORTS_DIR, LOGS_DIR, RUN_ID

# ── Page config ───────────────────────────────────────────────────────────────
st.set_page_config(
    page_title="SmartIngest — ETL Pipeline Dashboard",
    page_icon="⚙",
    layout="wide",
    initial_sidebar_state="expanded",
)

COLORS = {
    "bronze": "#CD7F32", "silver": "#A8A9AD", "gold": "#D4AF37",
    "blue": "#2563EB", "green": "#16A34A", "red": "#DC2626",
    "bg": "#F1F5F9", "card": "#FFFFFF",
}

# ── Custom CSS ────────────────────────────────────────────────────────────────
st.markdown("""
<style>
[data-testid="stAppViewContainer"] { background: #F1F5F9; }
[data-testid="stSidebar"] { background: #1E293B; }
[data-testid="stSidebar"] * { color: #F1F5F9 !important; }
.metric-card {
    background: white; border-radius: 12px; padding: 18px 20px;
    border: 1px solid #E2E8F0; margin-bottom: 12px;
}
.metric-value { font-size: 28px; font-weight: 700; color: #1E293B; }
.metric-label { font-size: 12px; color: #64748B; text-transform: uppercase; letter-spacing: 0.5px; }
.metric-delta { font-size: 13px; margin-top: 4px; }
.layer-badge {
    display: inline-block; padding: 3px 10px; border-radius: 20px;
    font-size: 12px; font-weight: 600; margin-right: 6px;
}
.bronze-badge { background: #FDE68A; color: #92400E; }
.silver-badge { background: #E2E8F0; color: #475569; }
.gold-badge   { background: #FEF3C7; color: #92400E; }
.status-ok  { color: #16A34A; font-weight: 600; }
.status-err { color: #DC2626; font-weight: 600; }
.section-header {
    font-size: 16px; font-weight: 700; color: #1E293B;
    border-bottom: 2px solid #2563EB; padding-bottom: 6px; margin: 20px 0 14px;
}
</style>
""", unsafe_allow_html=True)


# ── Sidebar ───────────────────────────────────────────────────────────────────
with st.sidebar:
    st.markdown("## ⚙ SmartIngest")
    st.markdown("**Multi-Source ETL Pipeline**")
    st.markdown("---")
    page = st.radio(
        "Navigation",
        ["Pipeline Overview", "Data Explorer", "SQL Workbench", "Run Reports", "Run Pipeline"],
        label_visibility="collapsed",
    )
    st.markdown("---")
    st.markdown("**Pipeline layers**")
    st.markdown('<span class="layer-badge bronze-badge">BRONZE</span> Raw ingest', unsafe_allow_html=True)
    st.markdown('<span class="layer-badge silver-badge">SILVER</span> Clean & validate', unsafe_allow_html=True)
    st.markdown('<span class="layer-badge gold-badge">GOLD</span> Analytics-ready', unsafe_allow_html=True)
    st.markdown("---")
    st.markdown("**Sources**")
    st.markdown("- Population by state (CSV)")
    st.markdown("- Literacy by state (CSV)")
    st.markdown("- Employment by state (CSV)")


# ── Load DB ───────────────────────────────────────────────────────────────────
@st.cache_data(ttl=30)
def load_data():
    if not os.path.exists(DB_PATH):
        return None
    conn = sqlite3.connect(DB_PATH)
    df = pd.read_sql(f"SELECT * FROM {TARGET_TABLE}", conn)
    conn.close()
    return df

df = load_data()
db_ready = df is not None and len(df) > 0


# ══════════════════════════════════════════════════════════════════════════════
# PAGE 1: Pipeline Overview
# ══════════════════════════════════════════════════════════════════════════════
if page == "Pipeline Overview":
    st.markdown("# Pipeline Overview")
    st.markdown("End-to-end ETL pipeline ingesting Indian Census data from 3 CSV sources into a validated SQLite database.")

    # Status row
    col1, col2, col3, col4 = st.columns(4)
    with col1:
        st.markdown(f"""<div class="metric-card">
            <div class="metric-label">Pipeline Status</div>
            <div class="metric-value">{'✓ Live' if db_ready else '✗ Not run'}</div>
            <div class="metric-delta {'status-ok' if db_ready else 'status-err'}">{'Database ready' if db_ready else 'Run pipeline first'}</div>
        </div>""", unsafe_allow_html=True)
    with col2:
        rows = len(df) if db_ready else 0
        st.markdown(f"""<div class="metric-card">
            <div class="metric-label">Rows Loaded</div>
            <div class="metric-value">{rows}</div>
            <div class="metric-delta" style="color:#64748B">30 Indian states</div>
        </div>""", unsafe_allow_html=True)
    with col3:
        cols = len(df.columns) if db_ready else 0
        st.markdown(f"""<div class="metric-card">
            <div class="metric-label">Features</div>
            <div class="metric-value">{cols}</div>
            <div class="metric-delta" style="color:#64748B">Merged + derived columns</div>
        </div>""", unsafe_allow_html=True)
    with col4:
        sources = 3
        st.markdown(f"""<div class="metric-card">
            <div class="metric-label">Data Sources</div>
            <div class="metric-value">{sources}</div>
            <div class="metric-delta" style="color:#64748B">CSV files merged</div>
        </div>""", unsafe_allow_html=True)

    # Architecture diagram
    st.markdown('<div class="section-header">Pipeline Architecture</div>', unsafe_allow_html=True)
    st.markdown("""
    | Stage | Module | Input | Output | Key operations |
    |---|---|---|---|---|
    | **Bronze** | `extract.py` | 3 CSV files | 3 raw DataFrames | Read, tag source, log nulls |
    | **Silver** | `transform.py` | Raw DataFrames | 1 clean DataFrame | Standardize, impute, merge, derive |
    | **Gold** | `load.py` | Clean DataFrame | SQLite table | Validate, commit, index |
    | **Report** | `report.py` | All stages | Charts + text | Visualize, summarize |
    """)

    if db_ready:
        st.markdown('<div class="section-header">Quick Insights</div>', unsafe_allow_html=True)
        col1, col2 = st.columns(2)

        with col1:
            # Dev index bar chart
            fig, ax = plt.subplots(figsize=(6, 5), facecolor="white")
            top10 = df.nlargest(10, "dev_index").sort_values("dev_index")
            colors = ["#2563EB" if i >= 7 else "#93C5FD" for i in range(10)]
            ax.barh(top10["state"], top10["dev_index"], color=colors)
            ax.set_xlabel("Development Index", fontsize=9)
            ax.set_title("Top 10 States — Development Index", fontsize=10, fontweight="bold")
            ax.spines[["top","right"]].set_visible(False)
            fig.tight_layout()
            st.pyplot(fig)
            plt.close(fig)

        with col2:
            # Scatter literacy vs income
            fig, ax = plt.subplots(figsize=(6, 5), facecolor="white")
            sc = ax.scatter(df["literacy_rate_overall"], df["per_capita_income_inr"]/1000,
                           c=df["dev_index"], cmap="RdYlGn", s=60, alpha=0.8, edgecolors="white")
            plt.colorbar(sc, ax=ax, label="Dev Index", shrink=0.8)
            ax.set_xlabel("Literacy Rate (%)", fontsize=9)
            ax.set_ylabel("Per Capita Income (₹K)", fontsize=9)
            ax.set_title("Literacy vs Income", fontsize=10, fontweight="bold")
            ax.spines[["top","right"]].set_visible(False)
            fig.tight_layout()
            st.pyplot(fig)
            plt.close(fig)


# ══════════════════════════════════════════════════════════════════════════════
# PAGE 2: Data Explorer
# ══════════════════════════════════════════════════════════════════════════════
elif page == "Data Explorer":
    st.markdown("# Data Explorer")
    if not db_ready:
        st.warning("No data loaded yet. Go to 'Run Pipeline' first.")
        st.stop()

    # Filters
    col1, col2, col3 = st.columns(3)
    with col1:
        min_lit, max_lit = float(df["literacy_rate_overall"].min()), float(df["literacy_rate_overall"].max())
        lit_range = st.slider("Literacy rate (%)", min_lit, max_lit, (min_lit, max_lit))
    with col2:
        min_inc, max_inc = float(df["per_capita_income_inr"].min()), float(df["per_capita_income_inr"].max())
        inc_range = st.slider("Per Capita Income (₹)", min_inc, max_inc, (min_inc, max_inc))
    with col3:
        sort_by = st.selectbox("Sort by", ["dev_index","literacy_rate_overall","per_capita_income_inr",
                                            "unemployment_rate_pct","total_population"])

    filtered = df[
        (df["literacy_rate_overall"].between(*lit_range)) &
        (df["per_capita_income_inr"].between(*inc_range))
    ].sort_values(sort_by, ascending=False)

    st.markdown(f'<div class="section-header">Showing {len(filtered)} states</div>', unsafe_allow_html=True)

    display_cols = ["state","dev_index","literacy_rate_overall","per_capita_income_inr",
                    "unemployment_rate_pct","total_population","urban_population_pct","gender_literacy_gap"]
    st.dataframe(
        filtered[display_cols].rename(columns={
            "dev_index": "Dev Index",
            "literacy_rate_overall": "Literacy %",
            "per_capita_income_inr": "Income (₹)",
            "unemployment_rate_pct": "Unemployment %",
            "total_population": "Population",
            "urban_population_pct": "Urban %",
            "gender_literacy_gap": "Gender Lit Gap",
        }).reset_index(drop=True),
        use_container_width=True,
        height=400,
    )

    # Distribution charts
    st.markdown('<div class="section-header">Distributions</div>', unsafe_allow_html=True)
    col1, col2, col3 = st.columns(3)
    for col, metric, label, color in [
        (col1, "literacy_rate_overall", "Literacy Rate %", "#2563EB"),
        (col2, "unemployment_rate_pct", "Unemployment %", "#DC2626"),
        (col3, "dev_index", "Development Index", "#16A34A"),
    ]:
        with col:
            fig, ax = plt.subplots(figsize=(3.5, 2.5), facecolor="white")
            ax.hist(filtered[metric], bins=10, color=color, alpha=0.75, edgecolor="white")
            ax.axvline(filtered[metric].mean(), color="#1E293B", linestyle="--", linewidth=1.2)
            ax.set_title(label, fontsize=9, fontweight="bold")
            ax.spines[["top","right"]].set_visible(False)
            ax.tick_params(labelsize=7)
            fig.tight_layout()
            st.pyplot(fig)
            plt.close(fig)


# ══════════════════════════════════════════════════════════════════════════════
# PAGE 3: SQL Workbench
# ══════════════════════════════════════════════════════════════════════════════
elif page == "SQL Workbench":
    st.markdown("# SQL Workbench")
    st.markdown(f"Run any SQL query against `{TARGET_TABLE}` in the SQLite database.")

    if not db_ready:
        st.warning("No data loaded yet. Go to 'Run Pipeline' first.")
        st.stop()

    PRESET_QUERIES = {
        "Top 10 by development index": f"SELECT state, ROUND(dev_index,4) AS dev_index, ROUND(literacy_rate_overall,2) AS literacy_pct, ROUND(per_capita_income_inr) AS income FROM {TARGET_TABLE} ORDER BY dev_index DESC LIMIT 10",
        "Gender literacy gap (worst 10)": f"SELECT state, ROUND(literacy_rate_male,2) AS male_pct, ROUND(literacy_rate_female,2) AS female_pct, ROUND(gender_literacy_gap,2) AS gap FROM {TARGET_TABLE} ORDER BY gender_literacy_gap DESC LIMIT 10",
        "Window fn: rank by literacy": f"SELECT state, ROUND(literacy_rate_overall,2) AS literacy, RANK() OVER (ORDER BY literacy_rate_overall DESC) AS literacy_rank FROM {TARGET_TABLE}",
        "CASE WHEN: economy type": f"SELECT state, ROUND(agriculture_employment_pct,1) AS agri_pct, CASE WHEN agriculture_employment_pct > 50 THEN 'Agriculture' WHEN services_employment_pct > 40 THEN 'Services' ELSE 'Mixed' END AS economy_type FROM {TARGET_TABLE} ORDER BY agri_pct DESC",
        "CTE: above national average": f"WITH avg AS (SELECT AVG(literacy_rate_overall) AS a, AVG(per_capita_income_inr) AS b FROM {TARGET_TABLE}) SELECT s.state, ROUND(s.literacy_rate_overall,2) AS lit, ROUND(s.per_capita_income_inr) AS income FROM {TARGET_TABLE} s, avg WHERE s.literacy_rate_overall > avg.a AND s.per_capita_income_inr > avg.b",
        "Custom query": "",
    }

    preset = st.selectbox("Preset queries", list(PRESET_QUERIES.keys()))
    default_sql = PRESET_QUERIES[preset]

    sql = st.text_area("SQL Query", value=default_sql, height=100,
                        placeholder=f"SELECT * FROM {TARGET_TABLE} LIMIT 5")

    col1, col2 = st.columns([1, 5])
    with col1:
        run_btn = st.button("Run Query", type="primary")

    if run_btn and sql.strip():
        try:
            conn = sqlite3.connect(DB_PATH)
            result = pd.read_sql(sql, conn)
            conn.close()
            st.success(f"{len(result)} rows returned")
            st.dataframe(result, use_container_width=True)

            # Auto chart if numeric columns present
            num_cols = result.select_dtypes(include="number").columns.tolist()
            str_cols = result.select_dtypes(include="object").columns.tolist()
            if num_cols and str_cols and len(result) <= 30:
                fig, ax = plt.subplots(figsize=(7, 3.5), facecolor="white")
                ax.barh(result[str_cols[0]].astype(str), result[num_cols[0]], color="#2563EB", alpha=0.8)
                ax.set_title(f"{str_cols[0]} vs {num_cols[0]}", fontsize=10, fontweight="bold")
                ax.spines[["top","right"]].set_visible(False)
                fig.tight_layout()
                st.pyplot(fig)
                plt.close(fig)

        except Exception as e:
            st.error(f"Query error: {e}")

    st.markdown('<div class="section-header">Available Columns</div>', unsafe_allow_html=True)
    st.code(", ".join(df.columns.tolist()), language=None)


# ══════════════════════════════════════════════════════════════════════════════
# PAGE 4: Run Reports
# ══════════════════════════════════════════════════════════════════════════════
elif page == "Run Reports":
    st.markdown("# Run Reports")
    report_files = sorted(glob.glob(os.path.join(REPORTS_DIR, "run_*.txt")), reverse=True)
    if not report_files:
        st.info("No reports found. Run the pipeline first.")
        st.stop()

    selected = st.selectbox("Select run", [os.path.basename(f) for f in report_files])
    selected_path = os.path.join(REPORTS_DIR, selected)
    with open(selected_path) as f:
        content = f.read()
    st.code(content, language=None)

    run_id = selected.replace("run_","").replace(".txt","")
    chart_dir = os.path.join(REPORTS_DIR, f"charts_{run_id}")
    if os.path.exists(chart_dir):
        st.markdown('<div class="section-header">Charts from this run</div>', unsafe_allow_html=True)
        chart_files = sorted(glob.glob(os.path.join(chart_dir, "*.png")))
        cols = st.columns(2)
        for i, cf in enumerate(chart_files):
            with cols[i % 2]:
                st.image(cf, use_container_width=True)


# ══════════════════════════════════════════════════════════════════════════════
# PAGE 5: Run Pipeline
# ══════════════════════════════════════════════════════════════════════════════
elif page == "Run Pipeline":
    st.markdown("# Run Pipeline")
    st.markdown("Trigger the full ETL pipeline from this dashboard.")

    col1, col2 = st.columns(2)
    with col1:
        st.markdown("""
        **What this does:**
        1. Generates fresh CSV datasets (if needed)
        2. Runs Extract → Transform → Load
        3. Generates a run report and 4 charts
        4. Loads results into SQLite
        """)
    with col2:
        st.markdown("""
        **Pipeline modules:**
        - `generate_data.py` — seed data
        - `extract.py` — Bronze layer
        - `transform.py` — Silver layer
        - `load.py` — Gold layer (SQLite)
        - `report.py` — Charts + summary
        """)

    gen_data = st.checkbox("Re-generate fresh data before running", value=not os.path.exists("data/raw/population_by_state.csv"))

    if st.button("Run ETL Pipeline", type="primary"):
        with st.spinner("Pipeline running..."):
            log_lines = []
            try:
                if gen_data:
                    result = subprocess.run([sys.executable, "generate_data.py"],
                                           capture_output=True, text=True, cwd=os.path.dirname(os.path.abspath(__file__)))
                    log_lines.append("DATA GENERATION:\n" + result.stdout)

                result = subprocess.run([sys.executable, "pipeline.py"],
                                       capture_output=True, text=True, cwd=os.path.dirname(os.path.abspath(__file__)))
                log_lines.append("PIPELINE OUTPUT:\n" + result.stdout)

                if result.returncode == 0:
                    st.success("Pipeline completed successfully!")
                    st.cache_data.clear()
                else:
                    st.error("Pipeline failed.")
                    log_lines.append("STDERR:\n" + result.stderr)

            except Exception as e:
                st.error(f"Error: {e}")
                log_lines.append(str(e))

            with st.expander("Full pipeline log", expanded=True):
                st.code("\n\n".join(log_lines), language=None)
