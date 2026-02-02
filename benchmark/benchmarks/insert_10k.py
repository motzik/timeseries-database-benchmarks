from __future__ import annotations

from dataclasses import dataclass
from time import perf_counter_ns
from datetime import datetime, timedelta
from typing import Dict, Any

from benchmark.db.base import Database, InsertRow, InsertBatch


@dataclass(frozen=True)
class BenchmarkRun:
    benchmark: str
    db: str
    params: Dict[str, Any]
    latency_ms: float
    row_count: int


def run_insert_10k(db: Database, db_name: str, start_ts: datetime, rows: int) -> BenchmarkRun:
    vehicle_id = db.create_new_vehicle()
    job_id = db.create_new_job(vehicle_id)

    end_ts = start_ts + timedelta(seconds=rows - 1)
    batch_rows = [
        InsertRow(timestamp=start_ts + timedelta(seconds=idx), tel_speed=10.0 + idx % 100 * 0.1)
        for idx in range(rows)
    ]
    batch = InsertBatch(
        job_id=job_id,
        vehicle_id=vehicle_id,
        start_ts=start_ts,
        end_ts=end_ts,
        marker=f"insert_{job_id}_{vehicle_id}_{start_ts.isoformat()}",
        rows=batch_rows
    )

    t0 = perf_counter_ns()
    result = db.insert_batch(batch)
    t1 = perf_counter_ns()

    db.clean_data(vehicle_id, job_id)

    return BenchmarkRun(
        benchmark="insert_10k",
        db=db_name,
        params={
            "rows": rows,
            "job_id": job_id,
            "vehicle_id": vehicle_id,
            "start_ts": start_ts.isoformat(),
            "end_ts": end_ts.isoformat(),
        },
        latency_ms=(t1 - t0) / 1_000_000.0,
        row_count=result.row_count,
    )
