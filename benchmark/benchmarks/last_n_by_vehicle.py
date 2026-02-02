from __future__ import annotations

from dataclasses import dataclass
from time import perf_counter_ns
from typing import Dict, Any

from benchmark.db.base import Database


@dataclass(frozen=True)
class BenchmarkRun:
    benchmark: str
    db: str
    params: Dict[str, Any]
    latency_ms: float
    row_count: int


def run_last_n_by_vehicle(db: Database, db_name: str, vehicle_id: int, n: int) -> BenchmarkRun:
    t0 = perf_counter_ns()
    result = db.last_n_by_vehicle(vehicle_id, n)
    t1 = perf_counter_ns()

    return BenchmarkRun(
        benchmark="job_full",
        db=db_name,
        params={"vehicle_id": vehicle_id, "n": n},
        latency_ms=(t1 - t0) / 1_000_000.0,
        row_count=result.row_count,
    )
