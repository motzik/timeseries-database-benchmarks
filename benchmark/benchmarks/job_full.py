from __future__ import annotations

from dataclasses import dataclass
from time import perf_counter_ns
from typing import Dict, Any

from db.base import Database


@dataclass(frozen=True)
class BenchmarkRun:
    benchmark: str
    db: str
    params: Dict[str, Any]
    latency_ms: float
    row_count: int


def run_job_full(db: Database, db_name: str, job_id: Any) -> BenchmarkRun:
    t0 = perf_counter_ns()
    result = db.job_full(job_id)
    t1 = perf_counter_ns()

    return BenchmarkRun(
        benchmark="job_full",
        db=db_name,
        params={"job_id": job_id},
        latency_ms=(t1 - t0) / 1_000_000.0,
        row_count=result.row_count,
    )
