from __future__ import annotations

import os
from dataclasses import dataclass
from typing import Optional
from uuid import uuid4

import psycopg2
import requests

from benchmark.db.base import BATCH_SIZE, Database, QueryResult, InsertBatch

JOB_FULL_SQL = """
               SELECT *
               FROM dataset
               WHERE job_id = %s
               ORDER BY timestamp ASC; \
               """

LAST_N_BY_VEHICLE_SQL = """
                        SELECT d.*
                        FROM dataset d
                                 JOIN job j ON d.job_id = CAST(j.id AS SYMBOL)
                        WHERE j.vehicle_id = %s
                        ORDER BY d.timestamp DESC
                            LIMIT %s;

                        """

DASHBOARD_SPEED_10M_SQL = """
                          SELECT d.timestamp     AS bucket,
                                 avg(d.telSpeed) AS avg_speed
                          FROM dataset d
                                   JOIN job j ON d.job_id = CAST(j.id AS SYMBOL)
                          WHERE j.vehicle_id = %s
                            AND d.timestamp >= %s
                            AND d.timestamp < %s SAMPLE BY 10m ALIGN TO CALENDAR
                          ORDER BY bucket ASC;

                          """

DASHBOARD_SPEED_10M_MULTI_SQL = """
                                SELECT timestamp_floor('10m', d.timestamp) AS bucket,
                                       j.vehicle_id                        AS vehicle_id,
                                       avg(d.telSpeed)                     AS avg_speed
                                FROM dataset d
                                         JOIN job j ON d.job_id = CAST(j.id AS SYMBOL)
                                WHERE j.vehicle_id IN ({vehicle_placeholders})
                                  AND d.timestamp >= %s
                                  AND d.timestamp < %s
                                GROUP BY bucket, vehicle_id
                                ORDER BY bucket ASC, vehicle_id ASC;

                                """


@dataclass
class QuestDBConfig:
    host: str = "localhost"
    port: int = 8812
    database: str = "qdb"

    @staticmethod
    def from_env(prefix: str = "QUESTDB") -> "QuestDBConfig":
        return QuestDBConfig(
            host=os.getenv(f"{prefix}_HOST", "localhost"),
            port=int(os.getenv(f"{prefix}_PORT", "8812")),
            database=os.getenv(f"{prefix}_DB", "qdb"),
        )


class QuestDBDatabase(Database):
    def __init__(self, config: QuestDBConfig):
        self.config = config
        self._conn: Optional[psycopg2.extensions.connection] = None

    def connect(self) -> None:
        if self._conn is not None:
            return

        self._conn = psycopg2.connect(
            host=self.config.host,
            port=self.config.port,
            dbname=self.config.database,
            user="admin",  # ignored by QuestDB, but required by psycopg2
            password="quest",  # ignored
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

        cur = self._conn.cursor()
        cur.execute(JOB_FULL_SQL, (job_id,))

        rows = cur.fetchall()
        return QueryResult(row_count=len(rows))

    def last_n_by_vehicle(self, vehicle_id: int, n: int) -> QueryResult:
        if self._conn is None:
            raise RuntimeError("Database connection is not established.")

        cur = self._conn.cursor()
        cur.execute(LAST_N_BY_VEHICLE_SQL, (vehicle_id, n,))

        rows = cur.fetchall()
        return QueryResult(row_count=len(rows))

    def dashboard_speed_10m(self, vehicle_id: int, start_ts, end_ts) -> QueryResult:
        if self._conn is None:
            raise RuntimeError("Database connection is not established.")

        cur = self._conn.cursor()
        cur.execute(DASHBOARD_SPEED_10M_SQL, (vehicle_id, start_ts, end_ts,))

        rows = cur.fetchall()
        return QueryResult(row_count=len(rows))

    def dashboard_speed_10m_multi(
            self,
            vehicle_ids: list[int],
            start_ts,
            end_ts,
    ) -> QueryResult:
        if self._conn is None:
            raise RuntimeError("Database connection is not established.")

        cur = self._conn.cursor()
        placeholders = ", ".join(["%s"] * len(vehicle_ids))
        sql = DASHBOARD_SPEED_10M_MULTI_SQL.format(vehicle_placeholders=placeholders)
        params = [str(vehicle_id) for vehicle_id in vehicle_ids] + [start_ts, end_ts]
        cur.execute(sql, params)

        rows = cur.fetchall()
        return QueryResult(row_count=len(rows))

    def create_new_vehicle(self) -> str:
        if self._conn is None:
            raise RuntimeError("Database connection is not established.")

        cur = self._conn.cursor()
        try:
            new_id = uuid4().hex
            name = f"bench_vehicle_{new_id}"
            cur.execute(
                "INSERT INTO vehicle (id, name) VALUES (%s, %s);",
                (new_id, name),
            )
            self._conn.commit()
            return new_id
        except Exception:
            self._conn.rollback()
            raise
        finally:
            cur.close()

    def create_new_job(self, vehicle_id: str) -> str:
        if self._conn is None:
            raise RuntimeError("Database connection is not established.")

        cur = self._conn.cursor()
        try:
            new_id = uuid4().hex
            cur.execute(
                "INSERT INTO job (id, vehicle_id) VALUES (%s, %s);",
                (new_id, str(vehicle_id)),
            )
            self._conn.commit()
            return new_id
        except Exception:
            self._conn.rollback()
            raise
        finally:
            cur.close()

    def clean_data(self, vehicle_id: int, job_id: int) -> None:
        return
        if self._conn is None:
            raise RuntimeError("Database connection is not established.")

        cur = self._conn.cursor()
        # delete statements are not supported in questdb, so we have to do a workaround to reset the db
        # as all the generated datasets are the only ones which are in 2026, we just copy the data to a new database without these datasets,
        # then dropping the old db and renaming the new database
        try:
            cur.execute("""
                        CREATE TABLE dataset_new
                        (
                            timestamp  TIMESTAMP,
                            job_id     SYMBOL,
                            telPulseCounterDin1 DOUBLE,
                            telPulseCounterDin2 DOUBLE,
                            telTotalOdometer DOUBLE,
                            telGsmSignal DOUBLE,
                            telExternalVoltage DOUBLE,
                            telBatteryVoltage DOUBLE,
                            telBatteryCurrent DOUBLE,
                            telGnssStatus DOUBLE,
                            telGnssPdop DOUBLE,
                            telGnssHdop DOUBLE,
                            telSleepMode DOUBLE,
                            telIgnition DOUBLE,
                            telMovement DOUBLE,
                            telActiveGsmOperator DOUBLE,
                            telAltitude DOUBLE,
                            telAngle DOUBLE,
                            telLatitude DOUBLE,
                            telLongitude DOUBLE,
                            telSattelites DOUBLE,
                            telSpeed DOUBLE
                        ) timestamp(timestamp) PARTITION BY DAY WAL;
                        """)

            cur.execute("""
                        INSERT INTO dataset_new
                        SELECT *
                        FROM dataset
                        WHERE timestamp < '2026-01-01T00:00:00';
                        """)
            cur.execute("DROP TABLE dataset;")
            cur.execute("RENAME TABLE dataset_new TO dataset;")

            self._conn.commit()
        except Exception:
            self._conn.rollback()
            raise
        finally:
            cur.close()

    def insert_batch(self, batch: InsertBatch) -> QueryResult:
        self.ilp_insert_batch(
            table="dataset",
            batch=batch
        )
        return QueryResult(row_count=len(batch.rows))

    # TODO: clean up this implementation
    def ilp_insert_batch(
            self,
            table: str,
            batch: InsertBatch,
    ):
        lines = []
        for r in batch.rows:
            ts_ns = int(r.timestamp.timestamp() * 1_000_000_000)
            line = (
                f"{table},job_id={batch.job_id} "
                f"telSpeed={r.tel_speed} {ts_ns}"
            )
            lines.append(line)

        url = "http://localhost:9000/write?precision=ns"
        headers = {
            "Content-Type": "text/plain; charset=utf-8",
            "Connection": "close",
        }

        for i in range(0, len(lines), BATCH_SIZE):
            payload = "\n".join(lines[i:i + BATCH_SIZE]) + "\n"
            resp = requests.post(
                url,
                data=payload.encode("utf-8"),
                headers=headers,
                timeout=(5, 30),
            )
            resp.raise_for_status()

            if resp.status_code != 204:
                raise RuntimeError(f"ILP insert failed: {resp.status_code} {resp.text}")
