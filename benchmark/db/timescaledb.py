from __future__ import annotations

import os
from dataclasses import dataclass
from datetime import datetime
from typing import Optional

import psycopg2

from benchmark.db.base import Database, QueryResult

JOB_FULL_SQL = """
               SELECT *
               FROM dataset
               WHERE job_id = %s
               ORDER BY timestamp ASC; \
               """

LAST_N_BY_VEHICLE_SQL = """
                        SELECT *
                        FROM dataset
                        WHERE vehicle_id = %s
                        ORDER BY timestamp DESC
                            LIMIT %s; \
                        """

DASHBOARD_SPEED_10M_SQL = """
                          SELECT time_bucket(INTERVAL '10 minutes', timestamp) AS bucket,
                                 avg(telSpeed)                                 AS avg_speed
                          FROM dataset
                          WHERE vehicle_id = %s
                            AND timestamp >= %s
                            AND timestamp
                              < %s
                          GROUP BY bucket
                          ORDER BY bucket ASC; \
                          """


@dataclass
class TimescaleDBConfig:
    host: str = "localhost"
    port: int = 5432
    database: str = "telemetry"
    user: str = "postgres"
    password: str = "postgres"
    sslmode: str = "disable"  # e.g. "require" if you ever use SSL

    @staticmethod
    def from_env(prefix: str = "TIMESCALEDB") -> "TimescaleDBConfig":
        return TimescaleDBConfig(
            host=os.getenv(f"{prefix}_HOST", "localhost"),
            port=int(os.getenv(f"{prefix}_PORT", "5432")),
            database=os.getenv(f"{prefix}_DATABASE", "postgres"),
            user=os.getenv(f"{prefix}_USER", "postgres"),
            password=os.getenv(f"{prefix}_PASSWORD", "postgres"),
            sslmode=os.getenv(f"{prefix}_SSLMODE", "disable"),
        )


class TimescaleDatabase(Database):
    def __init__(self, config: TimescaleDBConfig):
        self.config = config
        self._conn: Optional[psycopg2.extensions.connection] = None

    def connect(self) -> None:
        if self._conn is not None:
            return

        self._conn = psycopg2.connect(
            host=self.config.host,
            port=self.config.port,
            dbname=self.config.database,
            user=self.config.user,
            password=self.config.password,
            sslmode=self.config.sslmode,
        )
        self._conn.autocommit = True

    def close(self) -> None:
        if self._conn is None:
            return
        try:
            self._conn.close()
        finally:
            self._conn = None

    def job_full(self, job_id: int) -> QueryResult:
        if self._conn is None:
            raise RuntimeError("Database connection is not established.")

        with self._conn.cursor() as cur:
            cur.execute(JOB_FULL_SQL, (job_id,))
            rows = cur.fetchall()
        return QueryResult(row_count=len(rows))

    def last_n_by_vehicle(self, vehicle_id: int, n: int) -> QueryResult:
        if self._conn is None:
            raise RuntimeError("Database connection is not established.")

        with self._conn.cursor() as cur:
            cur.execute(LAST_N_BY_VEHICLE_SQL, (vehicle_id, n))
            rows = cur.fetchall()
        return QueryResult(row_count=len(rows))

    def dashboard_speed_10m(
            self,
            vehicle_id: int,
            start_ts: datetime,
            end_ts: datetime,
    ) -> QueryResult:
        if self._conn is None:
            raise RuntimeError("Database connection is not established.")

        with self._conn.cursor() as cur:
            cur.execute(DASHBOARD_SPEED_10M_SQL, (vehicle_id, start_ts, end_ts))
            rows = cur.fetchall()
        return QueryResult(row_count=len(rows))
