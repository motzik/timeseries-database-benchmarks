from __future__ import annotations

import os
from dataclasses import dataclass
from typing import Optional

import psycopg2

from benchmark.db.base import Database, QueryResult

JOB_FULL_SQL = """
               SELECT *
               FROM dataset
               WHERE job_id = %s
               ORDER BY timestamp ASC; \
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


class QuestDBWideDatabase(Database):
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
            user="admin",       # ignored by QuestDB, but required by psycopg2
            password="quest",   # ignored
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