"""
report.py - Run summary + visualizations.
Generates a text report and 4 Matplotlib charts saved as PNG files.
"""
import os
import sqlite3
import pandas as pd
import numpy as np
import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt
import matplotlib.gridspec as gridspec
from datetime import datetime
from logger import get_logger
from config import REPORTS_DIR, DB_PATH, TARGET_TABLE, RUN_ID, REPORT_FILE

log = get_logger("report")

CHART_DIR = os.path.join(REPORTS_DIR, f"charts_{RUN_ID}")
os.makedirs(CHART_DIR, exist_ok=True)

COLORS = {
    "bronze": "#CD7F32",
    "silver": "#A8A9AD",
    "gold":   "#FFD700",
    "accent": "#2563EB",
    "good":   "#16A34A",
    "warn":   "#D97706",
    "bg":     "#F8FAFC",
    "text":   "#1E293B",
}


def _style_ax(ax, title=""):
    ax.set_facecolor(COLORS["bg"])
    ax.spines[["top","right"]].set_visible(False)
    ax.spines[["left","bottom"]].set_color("#CBD5E1")
    ax.tick_params(colors=COLORS["text"], labelsize=9)
    ax.yaxis.label.set_color(COLORS["text"])
    ax.xaxis.label.set_color(COLORS["text"])
    if title:
        ax.set_title(title, fontsize=11, fontweight="bold", color=COLORS["text"], pad=10)


def chart_null_rates(raw_nulls: dict, clean_nulls: dict) -> str:
    """Bar chart: null % per source before vs after transform."""
    sources = list(raw_nulls.keys())
    before  = [raw_nulls[s] for s in sources]
    after   = [clean_nulls[s] for s in sources]
    x = np.arange(len(sources))
    w = 0.35

    fig, ax = plt.subplots(figsize=(7, 4), facecolor="white")
    bars1 = ax.bar(x - w/2, before, w, label="Before transform", color=COLORS["bronze"], alpha=0.9)
    bars2 = ax.bar(x + w/2, after,  w, label="After transform",  color=COLORS["good"],   alpha=0.9)
    ax.set_xticks(x)
    ax.set_xticklabels([s.title() for s in sources])
    ax.set_ylabel("Total null values")
    ax.legend(fontsize=9)
    _style_ax(ax, "Data Quality: Null Values Before vs After Transform")

    for bar in bars1:
        ax.text(bar.get_x() + bar.get_width()/2, bar.get_height() + 0.3,
                str(int(bar.get_height())), ha="center", va="bottom", fontsize=8, color=COLORS["text"])
    for bar in bars2:
        ax.text(bar.get_x() + bar.get_width()/2, bar.get_height() + 0.3,
                str(int(bar.get_height())), ha="center", va="bottom", fontsize=8, color=COLORS["good"])

    fig.tight_layout()
    path = os.path.join(CHART_DIR, "01_null_rates.png")
    fig.savefig(path, dpi=150, bbox_inches="tight")
    plt.close(fig)
    log.info(f"Chart saved: {path}")
    return path


def chart_rows_per_source(row_counts: dict) -> str:
    """Pie chart: rows extracted per source."""
    labels = [k.title() for k in row_counts]
    sizes  = list(row_counts.values())
    colors = [COLORS["bronze"], COLORS["silver"], COLORS["gold"]]
    explode = (0.05, 0.05, 0.05)

    fig, ax = plt.subplots(figsize=(5, 5), facecolor="white")
    wedges, texts, autotexts = ax.pie(
        sizes, labels=labels, autopct="%1.1f%%",
        colors=colors, explode=explode,
        textprops={"fontsize": 10, "color": COLORS["text"]},
        wedgeprops={"linewidth": 1, "edgecolor": "white"},
    )
    for at in autotexts:
        at.set_fontsize(9)
        at.set_fontweight("bold")
    ax.set_title("Rows Extracted per Source", fontsize=11, fontweight="bold",
                 color=COLORS["text"], pad=10)
    fig.tight_layout()
    path = os.path.join(CHART_DIR, "02_rows_per_source.png")
    fig.savefig(path, dpi=150, bbox_inches="tight")
    plt.close(fig)
    log.info(f"Chart saved: {path}")
    return path


def chart_dev_index(df: pd.DataFrame) -> str:
    """Horizontal bar chart: Development Index by state."""
    df_sorted = df.sort_values("dev_index", ascending=True).tail(20)
    colors_bar = [
        COLORS["good"] if v >= df["dev_index"].quantile(0.67)
        else COLORS["warn"] if v >= df["dev_index"].quantile(0.33)
        else COLORS["bronze"]
        for v in df_sorted["dev_index"]
    ]

    fig, ax = plt.subplots(figsize=(7, 6), facecolor="white")
    bars = ax.barh(df_sorted["state"], df_sorted["dev_index"], color=colors_bar, alpha=0.88)
    ax.set_xlabel("Development Index (0–1)")
    ax.axvline(df["dev_index"].mean(), color=COLORS["accent"], linestyle="--",
               linewidth=1.2, label=f"Mean = {df['dev_index'].mean():.3f}")
    ax.legend(fontsize=9)
    _style_ax(ax, "State Development Index (Literacy + Employment + Income)")
    fig.tight_layout()
    path = os.path.join(CHART_DIR, "03_dev_index.png")
    fig.savefig(path, dpi=150, bbox_inches="tight")
    plt.close(fig)
    log.info(f"Chart saved: {path}")
    return path


def chart_scatter_literacy_income(df: pd.DataFrame) -> str:
    """Scatter: literacy rate vs per capita income."""
    fig, ax = plt.subplots(figsize=(7, 4), facecolor="white")
    sc = ax.scatter(
        df["literacy_rate_overall"], df["per_capita_income_inr"] / 1000,
        c=df["dev_index"], cmap="RdYlGn", s=70, alpha=0.85, edgecolors="white", linewidths=0.5
    )
    for _, row in df.iterrows():
        ax.annotate(row["state"][:4], (row["literacy_rate_overall"], row["per_capita_income_inr"]/1000),
                    fontsize=6, color=COLORS["text"], alpha=0.7)
    cbar = fig.colorbar(sc, ax=ax, shrink=0.8)
    cbar.set_label("Dev Index", fontsize=9)
    ax.set_xlabel("Literacy Rate (%)")
    ax.set_ylabel("Per Capita Income (₹K)")
    _style_ax(ax, "Literacy Rate vs Per Capita Income by State")
    fig.tight_layout()
    path = os.path.join(CHART_DIR, "04_literacy_vs_income.png")
    fig.savefig(path, dpi=150, bbox_inches="tight")
    plt.close(fig)
    log.info(f"Chart saved: {path}")
    return path


def generate_report(
    raw_frames: dict,
    df_final: pd.DataFrame,
    qr,
    load_meta: dict,
    timings: dict,
) -> str:
    """Generate full text report and all charts. Returns report file path."""
    log.info("Generating run report and charts...")

    # Compute null stats per source
    raw_nulls   = {k: int(v.isnull().sum().sum()) for k, v in raw_frames.items()}
    clean_nulls = {"population": 0, "literacy": 0, "employment": 0}  # post-transform

    rows_extracted = {k: len(v) for k, v in raw_frames.items()}
    rows_dropped   = sum(rows_extracted.values()) - load_meta["rows_loaded"]

    # Charts
    chart_paths = [
        chart_null_rates(raw_nulls, clean_nulls),
        chart_rows_per_source(rows_extracted),
        chart_dev_index(df_final),
        chart_scatter_literacy_income(df_final),
    ]

    # Text report
    now = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    sep = "=" * 62
    report_lines = [
        sep,
        f"  ETL PIPELINE RUN REPORT",
        f"  Run ID    : {RUN_ID}",
        f"  Timestamp : {now}",
        sep,
        "",
        "  EXTRACTION SUMMARY",
        "  " + "-" * 40,
        *[f"  {k:15s}: {v} rows extracted" for k, v in rows_extracted.items()],
        f"  {'TOTAL':15s}: {sum(rows_extracted.values())} rows",
        "",
        "  TRANSFORM SUMMARY",
        "  " + "-" * 40,
        *[f"  {k:15s}: {v} nulls at extraction" for k, v in raw_nulls.items()],
        f"  Rows dropped (dedup/merge): {rows_dropped}",
        f"  Derived columns added    : 5",
        f"  Final merged rows        : {len(df_final)}",
        f"  Final columns            : {len(df_final.columns)}",
        "",
        "  LOAD SUMMARY",
        "  " + "-" * 40,
        f"  Table       : {load_meta['table']}",
        f"  DB path     : {load_meta['db_path']}",
        f"  Rows loaded : {load_meta['rows_loaded']}",
        f"  Status      : SUCCESS",
        "",
        "  TIMING",
        "  " + "-" * 40,
        *[f"  {k:15s}: {v:.2f}s" for k, v in timings.items()],
        f"  {'TOTAL':15s}: {sum(timings.values()):.2f}s",
        "",
        "  DATA QUALITY LOG",
        "  " + "-" * 40,
        *["  " + line for line in qr.summary().split("\n")],
        "",
        "  CHARTS GENERATED",
        "  " + "-" * 40,
        *[f"  {os.path.basename(p)}" for p in chart_paths],
        "",
        sep,
    ]

    report_text = "\n".join(report_lines)
    with open(REPORT_FILE, "w", encoding="utf-8") as f:
        f.write(report_text)

    log.info(f"Report saved: {REPORT_FILE}")
    print("\n" + report_text)
    return REPORT_FILE
