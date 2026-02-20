#!/usr/bin/env python3
import argparse
import json
import os
from pathlib import Path

import numpy as np
import pandas as pd
import matplotlib.pyplot as plt


DEFAULT_DB_ORDER = ["mssql_narrow", "mssql_wide", "timescaledb", "questdb", "influxdb"]
DEFAULT_BENCH_ORDER = [
    "insert_10k",
    "job_full",
    "last_n_by_vehicle",
    "dashboard_speed_10m",
    "dashboard_speed_10m_multi",
]

DB_LABELS = {
    "mssql_narrow": "MSSQL (Narrow)",
    "mssql_wide": "MSSQL (Wide)",
    "timescaledb": "TimescaleDB",
    "questdb": "QuestDB",
    "influxdb": "InfluxDB",
}



def parse_args():
    p = argparse.ArgumentParser(description="Generate thesis-ready benchmark plots (mean ± std) from results.csv")
    p.add_argument("--input", "-i", required=True, help="Path to results.csv")
    p.add_argument("--outdir", "-o", default="plots_out", help="Output directory for plots and summary")
    p.add_argument("--db-order", default=",".join(DEFAULT_DB_ORDER), help="Comma-separated DB order")
    p.add_argument("--bench-order", default=",".join(DEFAULT_BENCH_ORDER), help="Comma-separated benchmark order")
    p.add_argument("--warmup", action="store_true",
                   help="If set, treat runs with run_idx <= warmup_n as warmups and drop them (see --warmup-n).")
    p.add_argument("--warmup-n", type=int, default=2, help="Number of warmup runs per (db, benchmark) to drop.")
    p.add_argument("--title-prefix", default="", help="Optional prefix for plot titles")
    p.add_argument("--unit", choices=["ms", "s"], default="ms", help="Plot latency in milliseconds or seconds")
    p.add_argument("--dpi", type=int, default=300, help="DPI for PNG (if enabled)")
    p.add_argument("--also-png", action="store_true", help="Additionally export PNG (not recommended for LaTeX)")
    return p.parse_args()


def safe_filename(s: str) -> str:
    return "".join(c if c.isalnum() or c in ("-", "_") else "_" for c in s)


def main():
    args = parse_args()
    outdir = Path(args.outdir)
    outdir.mkdir(parents=True, exist_ok=True)

    db_order = [x.strip() for x in args.db_order.split(",") if x.strip()]
    bench_order = [x.strip() for x in args.bench_order.split(",") if x.strip()]

    df = pd.read_csv(args.input)

    required_cols = {"db", "benchmark", "run_idx", "latency_ms", "row_count"}
    missing = required_cols - set(df.columns)
    if missing:
        raise SystemExit(f"Missing required columns in CSV: {missing}")

    # Convert types defensively
    df["run_idx"] = pd.to_numeric(df["run_idx"], errors="coerce")
    df["latency_ms"] = pd.to_numeric(df["latency_ms"], errors="coerce")
    df["row_count"] = pd.to_numeric(df["row_count"], errors="coerce")

    df = df.dropna(subset=["db", "benchmark", "run_idx", "latency_ms"])

    # Optional warmup removal
    if args.warmup:
        df = df[df["run_idx"] > args.warmup_n].copy()

    # Unit conversion for plotting
    if args.unit == "s":
        df["latency_plot"] = df["latency_ms"] / 1000.0
        y_label = "Mean latency (s)"
    else:
        df["latency_plot"] = df["latency_ms"]
        y_label = "Mean latency (ms)"

    # Aggregate: mean/std + also keep mean row_count (for validation table)
    agg = (
        df.groupby(["benchmark", "db"], as_index=False)
          .agg(
              mean_latency=("latency_plot", "mean"),
              std_latency=("latency_plot", "std"),
              runs=("latency_plot", "count"),
              mean_row_count=("row_count", "mean"),
              min_row_count=("row_count", "min"),
              max_row_count=("row_count", "max"),
          )
    )

    # Save summary table
    summary_path = outdir / "summary.csv"
    agg.to_csv(summary_path, index=False)

    # Matplotlib "paper-ish" defaults (no custom colors)
    plt.rcParams.update({
        "figure.figsize": (7.2, 4.0),   # fits well on A4 with captions
        "font.size": 10,
        "axes.titlesize": 11,
        "axes.labelsize": 10,
        "xtick.labelsize": 9,
        "ytick.labelsize": 9,
        "legend.fontsize": 9,
        "axes.grid": True,
        "grid.alpha": 0.25,
        "grid.linestyle": "-",
        "axes.axisbelow": True,
        "savefig.bbox": "tight",
    })

    # Ensure ordering
    bench_list = [b for b in bench_order if b in agg["benchmark"].unique()] + \
                 [b for b in agg["benchmark"].unique() if b not in bench_order]

    for bench in bench_list:
        sub = agg[agg["benchmark"] == bench].copy()
        if sub.empty:
            continue

        # Order DBs
        sub["db"] = pd.Categorical(sub["db"], categories=db_order, ordered=True)
        sub = sub.sort_values("db")

        x = np.arange(len(sub))
        means = sub["mean_latency"].to_numpy()
        stds = sub["std_latency"].fillna(0).to_numpy()  # if std undefined (runs=1), show 0
        labels = [DB_LABELS.get(db, db) for db in sub["db"].astype(str)]

        fig, ax = plt.subplots()
        ax.bar(x, means, yerr=stds, capsize=4)

        ax.set_title("")
        ax.set_ylabel(y_label)
        ax.set_xticks(x)
        ax.set_xticklabels(labels, rotation=20, ha="right")

        # Export
        base = safe_filename(bench)
        pdf_path = outdir / f"{base}.pdf"
        svg_path = outdir / f"{base}.svg"
        fig.savefig(pdf_path)
        fig.savefig(svg_path)

        if args.also_png:
            png_path = outdir / f"{base}.png"
            fig.savefig(png_path, dpi=args.dpi)

        plt.close(fig)

    print(f"✅ Wrote plots to: {outdir}")
    print(f"✅ Wrote summary table to: {summary_path}")


if __name__ == "__main__":
    main()
