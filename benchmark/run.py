from __future__ import annotations

from dotenv import load_dotenv

import argparse
import csv
import os
from pathlib import Path
from datetime import datetime, timezone

from benchmark.benchmarks.job_full import (run_job_full, BenchmarkRun)
from benchmark.db.mssql_narrow import MSSQLNarrowDatabase
from benchmark.db.mssql_wide import MSSQLWideDatabase
from benchmark.db.mssql_config import MSSQLConfig
from benchmark.db.questdb import QuestDBConfig, QuestDBWideDatabase

ENV_PATH = Path(__file__).resolve().parent / ".env"
load_dotenv(dotenv_path=ENV_PATH, override=True)


def ensure_results_file(path: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    if not path.exists():
        with path.open("w", newline="", encoding="utf-8") as f:
            w = csv.writer(f)
            w.writerow([
                "ts_utc",
                "db",
                "benchmark",
                "run_idx",
                "latency_ms",
                "row_count",
                "params_json",
            ])


def append_result(path: Path, run_idx: int, r: BenchmarkRun) -> None:
    import json
    with path.open("a", newline="", encoding="utf-8") as f:
        w = csv.writer(f)
        w.writerow([
            datetime.now(timezone.utc).isoformat(),
            r.db,
            r.benchmark,
            run_idx,
            f"{r.latency_ms:.3f}",
            r.row_count,
            json.dumps(r.params, ensure_ascii=False),
        ])


def load_db(db_name: str):
    if db_name == "mssql_narrow":
        cfg = MSSQLConfig.from_env(prefix="MSSQL_NARROW")
        db = MSSQLNarrowDatabase(cfg)
        return db
    if db_name == "mssql_wide":
        cfg = MSSQLConfig.from_env(prefix="MSSQL_WIDE")
        db = MSSQLWideDatabase(cfg)
        return db
    if db_name == "questdb":
        cfg = QuestDBConfig.from_env(prefix="QUESTDB")
        db = QuestDBWideDatabase(cfg)
        return db
    else:
        raise ValueError(f"Unsupported db: {db_name}")


def main() -> int:
    p = argparse.ArgumentParser()
    p.add_argument("--db", required=True, choices=["mssql_narrow", "mssql_wide", "questdb"])
    p.add_argument("--benchmark", required=True, choices=["job_full"])
    p.add_argument("--job-id", type=int, default=int(os.environ.get("JOB_ID", "3137")))
    p.add_argument("--runs", type=int, default=5)
    p.add_argument("--warmup", type=int, default=1)
    p.add_argument("--out", type=str, default=str(Path("results") / "results.csv"))
    args = p.parse_args()

    out_path = Path(args.out)
    ensure_results_file(out_path)

    db = load_db(args.db)
    db.connect()

    try:
        # warmup runs (not recorded)
        for _ in range(max(0, args.warmup)):
            _ = run_job_full(db, db_name=args.db, job_id=args.job_id)

        # real runs
        for i in range(args.runs):
            r = run_job_full(db, db_name=args.db, job_id=args.job_id)
            append_result(out_path, i + 1, r)
            print(f"Run {i + 1}/{args.runs}: {r.latency_ms:.3f} ms, {r.row_count} rows")

    finally:
        db.close()
    print("results append to ", out_path)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
