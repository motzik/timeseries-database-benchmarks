from __future__ import annotations

from typing import Optional

import pyodbc

from benchmark.db.base import Database, QueryResult
from benchmark.db.mssql_config import MSSQLConfig

JOB_FULL_SQL = """
               SELECT *
               FROM dataset
               WHERE job_id = ?
               ORDER BY timestamp ASC; \
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