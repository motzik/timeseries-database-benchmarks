from __future__ import annotations

from dataclasses import dataclass
from time import perf_counter_ns
from datetime import datetime, timedelta
from typing import Dict, Any

from benchmark.db.base import BATCH_SIZE, Database, InsertRow, InsertBatch


def _build_sensor_payload(idx: int) -> dict[str, float]:
    return {
        "telAltitude": 130.0 + (idx % 20),
        "telAngle": float((idx * 7) % 360),
        "telExternalVoltage": 12150.0 + (idx % 50),
        "telLatitude": 48.42177 + (idx % 100) * 0.00001,
        "telLongitude": 14.14441 + (idx % 100) * 0.00001,
        "telMovement": float(idx % 2),
        "telPulseCounterDin1": float(idx * 2),
        "telPulseCounterDin2": float(idx * 3),
        "telSattelites": 10.0 + (idx % 8),
        "telSleepMode": float((idx // 120) % 2),
        "telSpeed": 10.0 + idx % 100 * 0.1,
        "telTotalOdometer": 13_132_959.0 + idx,
    }


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
        InsertRow(timestamp=start_ts + timedelta(seconds=idx), sensors=_build_sensor_payload(idx))
        for idx in range(rows)
    ]
    t0 = perf_counter_ns()
    row_count = 0
    marker = f"insert_{job_id}_{vehicle_id}_{start_ts.isoformat()}"
    for i in range(0, len(batch_rows), BATCH_SIZE):
        rows_chunk = batch_rows[i:i + BATCH_SIZE]
        chunk_batch = InsertBatch(
            job_id=job_id,
            vehicle_id=vehicle_id,
            start_ts=rows_chunk[0].timestamp,
            end_ts=rows_chunk[-1].timestamp,
            marker=marker,
            rows=rows_chunk,
        )
        row_count += db.insert_batch(chunk_batch).row_count

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
        row_count=row_count,
    )
