from __future__ import annotations

from typing import Optional

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
