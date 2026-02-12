from __future__ import annotations

import os
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any, Optional
from uuid import uuid4

from influxdb_client import InfluxDBClient, WritePrecision
from influxdb_client.client.query_api import QueryApi
from influxdb_client.client.write_api import WriteApi, SYNCHRONOUS

from benchmark.db.base import BATCH_SIZE, Database, InsertBatch, QueryResult

JOB_FULL_FLUX = """
from(bucket: "{bucket}")
  |> range(start: 0)
  |> filter(fn: (r) => r["_measurement"] == "{measurement}")
  |> filter(fn: (r) => r["_field"] == "telSpeed")
  |> filter(fn: (r) => r["job_id"] == "{job_id}")
  |> sort(columns: ["_time"], desc: false)
"""

LAST_N_BY_VEHICLE_FLUX = """
from(bucket: "{bucket}")
  |> range(start: 0)
  |> filter(fn: (r) => r["_measurement"] == "{measurement}")
  |> filter(fn: (r) => r["_field"] == "telSpeed")
  |> filter(fn: (r) => r["vehicle_id"] == "{vehicle_id}")
  |> group(columns: [])
  |> top(n: {n}, columns: ["_time"])
  |> sort(columns: ["_time"], desc: false)
"""

DASHBOARD_SPEED_10M_FLUX = """
from(bucket: "{bucket}")
  |> range(start: time(v: "{start_ts}"), stop: time(v: "{end_ts}"))
  |> filter(fn: (r) => r["_measurement"] == "{measurement}")
  |> filter(fn: (r) => r["vehicle_id"] == "{vehicle_id}")
  |> filter(fn: (r) => r["_field"] == "telSpeed")
  |> aggregateWindow(every: 10m, fn: mean, createEmpty: false)
  |> keep(columns: ["_time", "_value"])
  |> sort(columns: ["_time"], desc: false)
"""

DASHBOARD_SPEED_10M_MULTI_FLUX = """
from(bucket: "{bucket}")
  |> range(start: time(v: "{start_ts}"), stop: time(v: "{end_ts}"))
  |> filter(fn: (r) => r["_measurement"] == "{measurement}")
  |> filter(fn: (r) => r["_field"] == "telSpeed")
  |> filter(fn: (r) => {vehicle_filter})
  |> aggregateWindow(every: 10m, fn: mean, createEmpty: false)
  |> keep(columns: ["_time", "_value", "vehicle_id"])
  |> sort(columns: ["_time", "vehicle_id"], desc: false)
"""


@dataclass
class InfluxDBConfig:
    url: str = "http://localhost:8086"
    token: str = "change-me"
    org: str = "my-org"
    bucket: str = "telemetry"
    measurement: str = "dataset"
    timeout_ms: int = 30_000

    @staticmethod
    def from_env(prefix: str = "INFLUXDB") -> "InfluxDBConfig":
        return InfluxDBConfig(
            url=os.getenv(f"{prefix}_URL", "http://localhost:8086"),
            token=os.getenv(f"{prefix}_TOKEN", "change-me"),
            org=os.getenv(f"{prefix}_ORG", "my-org"),
            bucket=os.getenv(f"{prefix}_BUCKET", "telemetry"),
            measurement=os.getenv(f"{prefix}_MEASUREMENT", "dataset"),
            timeout_ms=int(os.getenv(f"{prefix}_TIMEOUT_MS", "30000")),
        )


class InfluxDBDatabase(Database):
    def __init__(self, config: InfluxDBConfig):
        self.config = config
        self._client: Optional[InfluxDBClient] = None
        self._query_api: Optional[QueryApi] = None
        self._write_api: Optional[WriteApi] = None

    def connect(self) -> None:
        if self._client is not None:
            return

        self._client = InfluxDBClient(
            url=self.config.url,
            token=self.config.token,
            org=self.config.org,
            timeout=self.config.timeout_ms,
        )
        self._query_api = self._client.query_api()
        self._write_api = self._client.write_api(write_options=SYNCHRONOUS)

    def close(self) -> None:
        if self._client is None:
            return
        try:
            self._client.close()
        finally:
            self._client = None
            self._query_api = None
            self._write_api = None

    def job_full(self, job_id: Any) -> QueryResult:
        query_api = self._require_query_api()
        flux_query = JOB_FULL_FLUX.format(
            bucket=self.config.bucket,
            measurement=self.config.measurement,
            job_id=job_id,
        )
        result = query_api.query(flux_query)
        row_count = _count_records(result)
        return QueryResult(row_count=row_count)

    def last_n_by_vehicle(self, vehicle_id: int, n: int) -> QueryResult:
        query_api = self._require_query_api()
        flux_query = LAST_N_BY_VEHICLE_FLUX.format(
            bucket=self.config.bucket,
            measurement=self.config.measurement,
            vehicle_id=vehicle_id,
            n=n,
        )
        result = query_api.query(flux_query)
        row_count = _count_records(result)
        return QueryResult(row_count=row_count)

    def dashboard_speed_10m(self, vehicle_id: int, start_ts: datetime, end_ts: datetime) -> QueryResult:
        query_api = self._require_query_api()
        flux_query = DASHBOARD_SPEED_10M_FLUX.format(
            bucket=self.config.bucket,
            measurement=self.config.measurement,
            vehicle_id=vehicle_id,
            start_ts=_to_rfc3339(start_ts),
            end_ts=_to_rfc3339(end_ts),
        )
        result = query_api.query(flux_query)
        row_count = _count_records(result)
        return QueryResult(row_count=row_count)

    def dashboard_speed_10m_multi(
            self,
            vehicle_ids: list[int],
            start_ts: datetime,
            end_ts: datetime,
    ) -> QueryResult:
        query_api = self._require_query_api()
        vehicle_id_set = "[" + ", ".join([f"\"{vehicle_id}\"" for vehicle_id in vehicle_ids]) + "]"
        vehicle_filter = " or ".join([f'r["vehicle_id"] == "{vid}"' for vid in vehicle_ids])
        flux_query = DASHBOARD_SPEED_10M_MULTI_FLUX.format(
            bucket=self.config.bucket,
            measurement=self.config.measurement,
            vehicle_ids=vehicle_id_set,
            start_ts=_to_rfc3339(start_ts),
            end_ts=_to_rfc3339(end_ts),
            vehicle_filter=vehicle_filter,
        )
        result = query_api.query(flux_query)
        row_count = _count_records(result)
        return QueryResult(row_count=row_count)

    def create_new_vehicle(self) -> str:
        return uuid4().hex

    def create_new_job(self, vehicle_id: int) -> str:
        return uuid4().hex

    def insert_batch(self, batch: InsertBatch) -> QueryResult:
        write_api = self._require_write_api()
        if not batch.rows:
            return QueryResult(row_count=0)

        lines = []
        for row in batch.rows:
            timestamp_ns = int(row.timestamp.timestamp() * 1_000_000_000)
            lines.append(
                f"{self.config.measurement},"
                f"job_id={batch.job_id},"
                f"vehicle_id={batch.vehicle_id} "
                f"telSpeed={row.tel_speed} "
                f"{timestamp_ns}"
            )

        for i in range(0, len(lines), BATCH_SIZE):
            payload = "\n".join(lines[i:i + BATCH_SIZE])
            write_api.write(
                bucket=self.config.bucket,
                org=self.config.org,
                record=payload,
                write_precision=WritePrecision.NS,
            )

        return QueryResult(row_count=len(batch.rows))

    def clean_data(self, vehicle_id: int, job_id: int) -> None:
        if self._client is None:
            raise RuntimeError("Database connection is not established.")

        delete_api = self._client.delete_api()
        start = "1970-01-01T00:00:00Z"
        stop = "2100-01-01T00:00:00Z"
        predicate = f'job_id="{job_id}" AND vehicle_id="{vehicle_id}"'
        delete_api.delete(
            start=start,
            stop=stop,
            predicate=predicate,
            bucket=self.config.bucket,
            org=self.config.org,
        )

    def _require_query_api(self) -> QueryApi:
        if self._query_api is None:
            raise RuntimeError("Database connection is not established.")
        return self._query_api

    def _require_write_api(self) -> WriteApi:
        if self._write_api is None:
            raise RuntimeError("Database connection is not established.")
        return self._write_api


def _to_rfc3339(ts: Any) -> str:
    if isinstance(ts, datetime):
        if ts.tzinfo is None:
            ts = ts.replace(tzinfo=timezone.utc)
        return ts.isoformat()

    s = str(ts)
    if s.endswith("Z") or "+" in s[10:]:
        return s

    return s + "Z"


def _count_records(query_result) -> int:
    count = 0
    for table in query_result:
        count += len(table.records)
    return count
