from __future__ import annotations

from dataclasses import dataclass
from typing import Optional

import pyodbc
import os
from db.base import Database, QueryResult

JOB_FULL_SQL = """
SELECT
    d.id           AS dataset_id,
    d.[timestamp]  AS dataset_timestamp,
    d.type         AS dataset_type,

    sr.id          AS sensor_record_id,
    sr.sensor_id,
    s.name         AS sensor_name,
    sr.value
FROM dbo.dataset d
    JOIN dbo.sensor_record sr
ON sr.dataset_id = d.id
    LEFT JOIN dbo.sensor s
    ON s.id = sr.sensor_id
WHERE d.job_id = ?
ORDER BY
    d.[timestamp] ASC,
    d.id ASC,
    sr.sensor_id ASC;
"""

@dataclass
class MSSQLConfig:
    host: str
    password: str
    port: int = 1433
    database: str = "telemetry_narrow"
    user: str = "sa"
    driver: str = "ODBC Driver 17 for SQL Server"
    encrypt: bool = False

    @staticmethod
    def from_env(prefix: str = "MSSQL_NARROW") -> MSSQLConfig:
        return MSSQLConfig(
            host=os.getenv(f"{prefix}_HOST", "localhost"),
            port=int(os.getenv(f"{prefix}_PORT", "1433")),
            database=os.getenv(f"{prefix}_DATABASE", "telemetry_narrow"),
            user=os.getenv(f"{prefix}_USER", "sa"),
            password=os.getenv(f"{prefix}_PASSWORD", ""),
            driver=os.environ.get(
                f"{prefix}_DRIVER",
                "ODBC Driver 18 for SQL Server",
            ),
            encrypt=os.environ.get(f"{prefix}_ENCRYPT", "false").lower() == "true",
        )

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