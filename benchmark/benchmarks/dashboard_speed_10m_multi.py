from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from time import perf_counter_ns
from typing import Any, Dict, Sequence

from benchmark.db.base import Database


@dataclass(frozen=True)
class BenchmarkRun:
    benchmark: str
    db: str
    params: Dict[str, Any]
    latency_ms: float
    row_count: int


def run_dashboard_speed_10m_multi(
    db: Database,
    db_name: str,
    vehicle_ids: Sequence[int],
    start_ts: datetime,
    end_ts: datetime,
) -> BenchmarkRun:
    t0 = perf_counter_ns()
    res = db.dashboard_speed_10m_multi(vehicle_ids=vehicle_ids, start_ts=start_ts, end_ts=end_ts)
    t1 = perf_counter_ns()

    return BenchmarkRun(
        benchmark="dashboard_speed_10m_multi",
        db=db_name,
        params={
            "vehicle_ids": list(vehicle_ids),
            "start_ts": start_ts.isoformat(),
            "end_ts": end_ts.isoformat(),
            "bucket": "10m",
            "metric": "avg(telSpeed)",
        },
        latency_ms=(t1 - t0) / 1_000_000.0,
        row_count=res.row_count,
    )
