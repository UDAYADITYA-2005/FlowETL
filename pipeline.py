"""
pipeline.py - Main orchestrator.
Run: python pipeline.py
Executes Extract -> Transform -> Load -> Report in sequence.
"""
import time
from logger import get_logger
from extract import extract_all
from transform import transform_all
from load import load
from report import generate_report
from config import RUN_ID

log = get_logger("pipeline")


def run():
    log.info("*" * 62)
    log.info(f"  PIPELINE STARTED  |  Run ID: {RUN_ID}")
    log.info("*" * 62)

    timings = {}
    total_start = time.time()

    # ── EXTRACT ───────────────────────────────────────────────────
    t0 = time.time()
    raw_frames = extract_all()
    timings["extract"] = time.time() - t0

    # ── TRANSFORM ─────────────────────────────────────────────────
    t0 = time.time()
    df_final, quality_report = transform_all(raw_frames)
    timings["transform"] = time.time() - t0

    # ── LOAD ──────────────────────────────────────────────────────
    t0 = time.time()
    load_meta = load(df_final)
    timings["load"] = time.time() - t0

    # ── REPORT ────────────────────────────────────────────────────
    t0 = time.time()
    report_path = generate_report(raw_frames, df_final, quality_report, load_meta, timings)
    timings["report"] = time.time() - t0

    total = time.time() - total_start
    log.info("*" * 62)
    log.info(f"  PIPELINE COMPLETE  |  Total time: {total:.2f}s")
    log.info(f"  Report: {report_path}")
    log.info("*" * 62)

    return df_final


if __name__ == "__main__":
    run()
