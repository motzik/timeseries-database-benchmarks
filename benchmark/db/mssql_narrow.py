from __future__ import annotations

from typing import Optional
from datetime import datetime

import pyodbc
from benchmark.db.base import Database, QueryResult
from benchmark.db.mssql_config import MSSQLConfig

JOB_FULL_SQL = """
               SELECT d.id AS dataset_id,
                      d.[timestamp] AS dataset_timestamp, d.type AS dataset_type, sr.id AS sensor_record_id, sr.sensor_id, s.name AS sensor_name, sr.value
               FROM dbo.dataset d
                   JOIN dbo.sensor_record sr
               ON sr.dataset_id = d.id
                   LEFT JOIN dbo.sensor s
                   ON s.id = sr.sensor_id
               WHERE d.job_id = ?
               ORDER BY
                   d.[timestamp] ASC,
                   d.id ASC,
                   sr.sensor_id ASC; \
               """

LAST_N_BY_VEHICLE_SQL = """
                        WITH last_ds AS (SELECT TOP(?) d.id, d.[timestamp]
                        FROM dbo.dataset d
                            LEFT JOIN dbo.job j
                        ON j.id = d.job_id
                        WHERE j.vehicle_id = ?
                        ORDER BY d.[timestamp] DESC, d.id DESC
                            )
                        SELECT d.id AS dataset_id,
                               d.[timestamp] AS dataset_timestamp, sr.id AS sensor_record_id, sr.sensor_id, s.name AS sensor_name, sr.value
                        FROM last_ds d
                            JOIN dbo.sensor_record sr
                        ON sr.dataset_id = d.id
                            LEFT JOIN dbo.sensor s
                            ON s.id = sr.sensor_id
                        ORDER BY
                            d.timestamp DESC,
                            d.id DESC,
                            sr.sensor_id ASC; \
 \
                        """

DASHBOARD_SPEED_10M_SQL = """
                          SELECT DATEADD(minute, (DATEDIFF(minute, 0, d.[timestamp]) / 10) * 10, 0) AS bucket,
                                 AVG(CAST(sr.value AS float))                                       AS avg_speed
                          FROM dbo.dataset d
                                   JOIN dbo.sensor_record sr
                                        ON sr.dataset_id = d.id
                                   JOIN dbo.job j
                                        ON j.id = d.job_id
                          WHERE j.vehicle_id = ?
                            AND d.[timestamp] >= ?
                            AND d.[timestamp]
                              < ?
                            AND sr.sensor_id = 45
                          GROUP BY DATEADD(minute, (DATEDIFF(minute, 0, d.[timestamp]) / 10) * 10, 0)
                          ORDER BY bucket ASC;
                          """

DASHBOARD_SPEED_10M_MULTI_SQL = """
                                SELECT DATEADD(minute, (DATEDIFF(minute, 0, d.[timestamp]) / 10) * 10, 0) AS bucket,
                                       j.vehicle_id                                                      AS vehicle_id,
                                       AVG(CAST(sr.value AS float))                                       AS avg_speed
                                FROM dbo.dataset d
                                         JOIN dbo.sensor_record sr
                                              ON sr.dataset_id = d.id
                                         JOIN dbo.job j
                                              ON j.id = d.job_id
                                WHERE j.vehicle_id IN ({vehicle_placeholders})
                                  AND d.[timestamp] >= ?
                                  AND d.[timestamp]
                                    < ?
                                  AND sr.sensor_id = 45
                                GROUP BY DATEADD(minute, (DATEDIFF(minute, 0, d.[timestamp]) / 10) * 10, 0),
                                         j.vehicle_id
                                ORDER BY bucket ASC, j.vehicle_id ASC;
                                """

NEXT_DATASET_ID_SQL = "SELECT ISNULL(MAX(id), 0) + 1 FROM dbo.dataset;"
NEXT_SENSOR_RECORD_ID_SQL = "SELECT ISNULL(MAX(id), 0) + 1 FROM dbo.sensor_record;"

INSERT_DATASET_SQL = """
INSERT INTO dbo.dataset (id, job_id, [timestamp], note, type)
VALUES (?, ?, ?, ?, ?);
"""

INSERT_SENSOR_RECORD_SQL = """
INSERT INTO dbo.sensor_record (id, value, sensor_id, dataset_id)
VALUES (?, ?, ?, ?);
"""


class MSSQLNarrowDatabase(Database):
    def __init__(self, config: MSSQLConfig):
        self.config = config
        self._conn: Optional[pyodbc.Connection] = None

    def connect(self) -> None:
        if self._conn is not None:
            return

        encrypt = "yes" if self.config.encrypt else "no"
        conn_str = (
            f"DRIVER={{{self.config.driver}}};"
            f"SERVER={self.config.host},{self.config.port};"
            f"DATABASE={self.config.database};"
            f"UID={self.config.user};"
            f"PWD={self.config.password};"
            f"Encrypt={encrypt};"
            "TrustServerCertificate=yes;"
        )
        self._conn = pyodbc.connect(conn_str)

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

        cursor = self._conn.cursor()
        cursor.execute(JOB_FULL_SQL, (job_id,))

        rows = cursor.fetchall()
        return QueryResult(row_count=len(rows))

    def last_n_by_vehicle(self, vehicle_id: int, n: int) -> QueryResult:
        if self._conn is None:
            raise RuntimeError("Database connection is not established.")

        cursor = self._conn.cursor()
        cursor.execute(LAST_N_BY_VEHICLE_SQL, (n, vehicle_id,))

        rows = cursor.fetchall()
        return QueryResult(row_count=len(rows))

    def dashboard_speed_10m(self, vehicle_id: int, start_ts: datetime, end_ts: datetime) -> QueryResult:
        if self._conn is None:
            raise RuntimeError("Database connection is not established.")

        cursor = self._conn.cursor()
        cursor.execute(DASHBOARD_SPEED_10M_SQL, (vehicle_id, start_ts, end_ts,))

        rows = cursor.fetchall()
        return QueryResult(row_count=len(rows))

    def dashboard_speed_10m_multi(
        self,
        vehicle_ids: list[int],
        start_ts: datetime,
        end_ts: datetime,
    ) -> QueryResult:
        if self._conn is None:
            raise RuntimeError("Database connection is not established.")

        cursor = self._conn.cursor()
        placeholders = ", ".join(["?"] * len(vehicle_ids))
        sql = DASHBOARD_SPEED_10M_MULTI_SQL.format(vehicle_placeholders=placeholders)
        params = list(vehicle_ids) + [start_ts, end_ts]
        cursor.execute(sql, params)

        rows = cursor.fetchall()
        return QueryResult(row_count=len(rows))

    def create_new_vehicle(self) -> int:
        if self._conn is None:
            raise RuntimeError("Database connection is not established.")

        cur = self._conn.cursor()

        cur.execute("SELECT ISNULL(MAX(id), 0) + 1 FROM dbo.vehicle;")
        vehicle_id = int(cur.fetchone()[0])

        cur.execute(
            """
            INSERT INTO dbo.vehicle (id,
                                     name,
                                     price_per_unit,
                                     price_per_km,
                                     price_per_hour,
                                     mwst,
                                     cubic_meter_per_cycle,
                                     active,
                                     creation,
                                     changed,
                                     version)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?);
            """,
            (  # some dummy data because of non null fields
                vehicle_id,
                f"benchmark_vehicle_{vehicle_id}",
                1.0,
                1.0,
                1.0,
                0.20,
                1.0,
                1,
                datetime.now(),
                datetime.now(),
                1,
            )
        )

        self._conn.commit()
        return vehicle_id

    def create_new_job(self, vehicle_id: int) -> int:
        if self._conn is None:
            raise RuntimeError("Database connection is not established.")

        cur = self._conn.cursor()

        cur.execute("SELECT ISNULL(MAX(id), 0) + 1 FROM dbo.job;")
        job_id = int(cur.fetchone()[0])

        cur.execute(
            """
            INSERT INTO dbo.job (id,
                                 vehicle_id,
                                 local_id,
                                 state)
            VALUES (?, ?, '1', '1');
            """,
            (job_id, vehicle_id)
        )

        self._conn.commit()
        return job_id

    def clean_data(self, vehicle_id: int, job_id: int) -> None:
        if self._conn is None:
            raise RuntimeError("Database connection is not established.")

        cur = self._conn.cursor()

        cur.execute("""
            DELETE sr
            FROM dbo.sensor_record sr
            JOIN dbo.dataset d
                ON d.id = sr.dataset_id
            JOIN dbo.job j
                ON j.id = d.job_id
            WHERE j.vehicle_id = ?;
        """, (vehicle_id,))

        cur.execute("""
            DELETE d
            FROM dbo.dataset d
            JOIN dbo.job j
                ON j.id = d.job_id
            WHERE j.vehicle_id = ?;
        """, (vehicle_id,))

        cur.execute(
            "DELETE FROM dbo.job WHERE id = ?;",
            (job_id,)
        )

        cur.execute(
            "DELETE FROM dbo.vehicle WHERE id = ?;",
            (vehicle_id,)
        )

        self._conn.commit()

    def insert_batch(self, batch) -> QueryResult:
        if self._conn is None:
            raise RuntimeError("Database connection is not established.")

        if not batch.rows:
            return QueryResult(row_count=0)

        SPEED_SENSOR_ID = 45

        cur = self._conn.cursor()

        cur.execute(NEXT_DATASET_ID_SQL)
        next_dataset_id = int(cur.fetchone()[0])

        cur.execute(NEXT_SENSOR_RECORD_ID_SQL)
        next_sr_id = int(cur.fetchone()[0])

        dataset_params: list[tuple] = []
        sr_params: list[tuple] = []

        for idx, r in enumerate(batch.rows):
            ds_id = next_dataset_id + idx
            sr_id = next_sr_id + idx

            dataset_params.append((
                ds_id,
                batch.job_id,
                r.timestamp,
                batch.marker,
                0,
            ))

            val = None if r.tel_speed is None else str(r.tel_speed)
            #TODO: maybe add more sensors for fair comparison
            sr_params.append((
                sr_id,
                val,
                SPEED_SENSOR_ID,
                ds_id,
            ))
        cur.fast_executemany = True

        try:
            cur.executemany(INSERT_DATASET_SQL, dataset_params)
            cur.executemany(INSERT_SENSOR_RECORD_SQL, sr_params)
            self._conn.commit()
        except Exception:
            self._conn.rollback()
            raise

        return QueryResult(row_count=len(dataset_params))

