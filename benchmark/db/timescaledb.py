from __future__ import annotations

import os
from dataclasses import dataclass
from datetime import datetime
from typing import Optional
from uuid import uuid4

import psycopg2
from psycopg2.extras import execute_values

from benchmark.db.base import Database, QueryResult, InsertBatch

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

DASHBOARD_SPEED_10M_MULTI_SQL = """
                                SELECT time_bucket(INTERVAL '10 minutes', timestamp) AS bucket,
                                       vehicle_id,
                                       avg(telSpeed)                                 AS avg_speed
                                FROM dataset
                                WHERE vehicle_id = ANY(%s)
                                  AND timestamp >= %s
                                  AND timestamp
                                    < %s
                                GROUP BY bucket, vehicle_id
                                ORDER BY bucket ASC, vehicle_id ASC; \
                                """

CREATE_VEHICLE_SQL = """
                     INSERT INTO vehicle (name)
                     VALUES (%s) RETURNING id; \
                     """

CREATE_JOB_SQL = """
                 INSERT INTO job (vehicle_id)
                 VALUES (%s) RETURNING id; \
                 """

INSERT_BATCH_SQL = """
                   INSERT INTO public.dataset (timestamp,
                                               vehicle_id,
                                               job_id,
                                               telspeed,
                                               string)
                   VALUES %s; \
                   """

DELETE_DATASET_BY_JOB_SQL = """
                            DELETE
                            FROM dataset
                            WHERE job_id = %s; \
                            """

DELETE_JOB_BY_ID_SQL = """
                       DELETE
                       FROM job
                       WHERE id = %s; \
                       """

DELETE_VEHICLE_BY_ID_SQL = """
                           DELETE
                           FROM vehicle
                           WHERE id = %s; \
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
        self._conn.autocommit = False

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

    def dashboard_speed_10m_multi(
        self,
        vehicle_ids: list[int],
        start_ts: datetime,
        end_ts: datetime,
    ) -> QueryResult:
        if self._conn is None:
            raise RuntimeError("Database connection is not established.")

        with self._conn.cursor() as cur:
            cur.execute(DASHBOARD_SPEED_10M_MULTI_SQL, (vehicle_ids, start_ts, end_ts))
            rows = cur.fetchall()
        return QueryResult(row_count=len(rows))

    def create_new_vehicle(self) -> int:
        if self._conn is None:
            raise RuntimeError("Database connection is not established.")

        vehicle_name = f"bench_vehicle_{uuid4().hex}"

        with self._conn.cursor() as cur:
            try:
                cur.execute(CREATE_VEHICLE_SQL, (vehicle_name,))
                row = cur.fetchone()
                self._conn.commit()
            except Exception:
                self._conn.rollback()
                raise

        if row is None:
            raise RuntimeError("Failed to create vehicle (no id returned).")

        return int(row[0])

    def create_new_job(self, vehicle_id: int) -> int:
        if self._conn is None:
            raise RuntimeError("Database connection is not established.")

        with self._conn.cursor() as cur:
            try:
                cur.execute(CREATE_JOB_SQL, (vehicle_id,))
                row = cur.fetchone()
                self._conn.commit()
            except Exception:
                self._conn.rollback()
                raise

        if row is None:
            raise RuntimeError("Failed to create job (no id returned).")

        return int(row[0])

    def insert_batch(self, batch: InsertBatch) -> QueryResult:
        if self._conn is None:
            raise RuntimeError("Database connection is not established.")

        if not batch.rows:
            return QueryResult(row_count=0)

        values: list[tuple] = []
        for r in batch.rows:
            values.append((
                r.timestamp,
                batch.vehicle_id,
                batch.job_id,
                r.tel_speed,
                batch.marker,
            ))

        with self._conn.cursor() as cur:
            try:
                execute_values(
                    cur,
                    INSERT_BATCH_SQL,
                    values,
                    page_size=1000,
                )
                self._conn.commit()
            except Exception:
                self._conn.rollback()
                raise

        return QueryResult(row_count=len(values))

    def clean_data(self, vehicle_id: int, job_id: int) -> None:
        if self._conn is None:
            raise RuntimeError("Database connection is not established.")

        with self._conn.cursor() as cur:
            try:
                cur.execute(DELETE_DATASET_BY_JOB_SQL, (job_id,))
                cur.execute(DELETE_JOB_BY_ID_SQL, (job_id,))
                cur.execute(DELETE_VEHICLE_BY_ID_SQL, (vehicle_id,))
                self._conn.commit()
            except Exception:
                self._conn.rollback()
                raise
