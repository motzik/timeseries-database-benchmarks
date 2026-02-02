from __future__ import annotations

from typing import Optional
from uuid import uuid4

import pyodbc

from benchmark.db.base import Database, QueryResult
from benchmark.db.mssql_config import MSSQLConfig

JOB_FULL_SQL = """
               SELECT *
               FROM dataset
               WHERE job_id = ?
               ORDER BY timestamp ASC;
               """

LAST_N_BY_VEHICLE_SQL = """
                        SELECT *
                        FROM dataset
                        WHERE vehicle_id = ?
                        ORDER BY timestamp DESC
                        OFFSET 0 ROWS FETCH NEXT ? ROWS ONLY;
                        """

DASHBOARD_SPEED_10M_SQL = """
                          SELECT DATEADD(
                                     minute, (DATEDIFF(minute, 0, [timestamp]) / 10) * 10, 0) AS bucket,
                                 AVG(TRY_CAST(telSpeed AS float))                             AS avg_speed
                          FROM dbo.dataset
                          WHERE vehicle_id = ?
                            AND [timestamp] >= ?
                            AND [timestamp]
                              < ?
                          GROUP BY DATEADD(
                              minute,
                              (DATEDIFF(minute, 0, [timestamp]) / 10) * 10, 0)
                          ORDER BY bucket ASC;
                          """

DASHBOARD_SPEED_10M_MULTI_SQL = """
                                SELECT DATEADD(
                                           minute, (DATEDIFF(minute, 0, [timestamp]) / 10) * 10, 0) AS bucket,
                                       vehicle_id                                                     AS vehicle_id,
                                       AVG(TRY_CAST(telSpeed AS float))                               AS avg_speed
                                FROM dbo.dataset
                                WHERE vehicle_id IN ({vehicle_placeholders})
                                  AND [timestamp] >= ?
                                  AND [timestamp]
                                    < ?
                                GROUP BY DATEADD(
                                    minute,
                                    (DATEDIFF(minute, 0, [timestamp]) / 10) * 10, 0),
                                         vehicle_id
                                ORDER BY bucket ASC, vehicle_id ASC;
                                """

INSERT_BATCH_SQL = """
                   INSERT INTO dbo.dataset (job_id,
                                            vehicle_id,
                       [ timestamp],
                       [ type],
                                            note,
                                            telSpeed)
                   VALUES (?, ?, ?, ?, ?, ?);
                   """

CREATE_VEHICLE_SQL = """
                     INSERT INTO dbo.vehicle (name, active)
                         OUTPUT INSERTED.id
                     VALUES (?, 1);
                     """

CREATE_JOB_SQL = """
                 INSERT INTO dbo.job (vehicle_id)
                     OUTPUT INSERTED.id
                 VALUES (?);
                 """

DELETE_VEHICLE_SQL = """
                     DELETE
                     FROM dbo.dataset
                     WHERE vehicle_id = ?;
                     DELETE
                     FROM dbo.job
                     WHERE vehicle_id = ?;
                     DELETE
                     FROM dbo.vehicle
                     WHERE id = ?;
                     """

DELETE_DATASET_SQL = """
                     DELETE
                     FROM dbo.dataset
                     WHERE job_id = ?; \
                     """

DELETE_JOB_SQL = """
                 DELETE
                 FROM dbo.job
                 WHERE id = ?; \
                 """


class MSSQLWideDatabase(Database):
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
        cursor.execute(LAST_N_BY_VEHICLE_SQL, (vehicle_id, n,))

        rows = cursor.fetchall()
        return QueryResult(row_count=len(rows))

    def dashboard_speed_10m(self, vehicle_id: int, start_ts, end_ts) -> QueryResult:
        if self._conn is None:
            raise RuntimeError("Database connection is not established.")

        cursor = self._conn.cursor()
        cursor.execute(DASHBOARD_SPEED_10M_SQL, (vehicle_id, start_ts, end_ts,))

        rows = cursor.fetchall()
        return QueryResult(row_count=len(rows))

    def dashboard_speed_10m_multi(
        self,
        vehicle_ids: list[int],
        start_ts,
        end_ts,
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

    def insert_batch(self, batch) -> QueryResult:
        if self._conn is None:
            raise RuntimeError("Database connection is not established.")

        if not batch.rows:
            return QueryResult(row_count=0)

        params: list[tuple] = []
        marker = batch.marker
        job_id = batch.job_id
        vehicle_id = batch.vehicle_id

        for r in batch.rows:
            # because speed is string in db
            tel_speed_str = None if r.tel_speed is None else str(r.tel_speed)
            params.append((job_id, vehicle_id, r.timestamp, 0, marker, tel_speed_str))

        cur = self._conn.cursor()
        cur.fast_executemany = True

        try:
            cur.executemany(INSERT_BATCH_SQL, params)
            self._conn.commit()
        except Exception:
            self._conn.rollback()
            raise

        return QueryResult(row_count=len(params))

    def create_new_vehicle(self) -> int:
        if self._conn is None:
            raise RuntimeError("Database connection is not established.")

        vehicle_name = f"bench_vehicle_{uuid4().hex}"

        cur = self._conn.cursor()
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

        cur = self._conn.cursor()
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

    def clean_data(self, vehicle_id: int, job_id) -> None:
        if self._conn is None:
            raise RuntimeError("Database connection is not established.")

        cur = self._conn.cursor()
        cur.execute(DELETE_DATASET_SQL, (job_id,))
        cur.execute(DELETE_JOB_SQL, (job_id,))
        cur.execute(DELETE_VEHICLE_SQL, (vehicle_id, vehicle_id, vehicle_id))
        self._conn.commit()
