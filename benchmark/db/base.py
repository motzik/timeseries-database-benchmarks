from abc import ABC, abstractmethod
from typing import Any, Iterable, Optional, Sequence
from dataclasses import dataclass
from datetime import datetime


BATCH_SIZE = 500


@dataclass(frozen=True)
class QueryResult:
    """Result metadata returned by database adapters"""
    row_count: int


@dataclass(frozen=True)
class InsertRow:
    timestamp: datetime
    tel_speed: float


@dataclass(frozen=True)
class InsertBatch:
    job_id: int
    vehicle_id: int
    start_ts: datetime
    end_ts: datetime
    rows: Sequence[InsertRow]
    marker: str


class Database(ABC):
    """
    Abstract database interface.
    Each database systeme must implement this interface.
    """

    @abstractmethod
    def connect(self) -> None:
        """Establish a connection to the database."""
        pass

    @abstractmethod
    def close(self) -> None:
        """Close the connection to the database."""
        pass

    @abstractmethod
    def job_full(self, job_id: Any) -> QueryResult:
        """
        Benchmark query #x: Full data for a job.
        """
        raise NotImplementedError

    @abstractmethod
    def last_n_by_vehicle(self, vehicle_id: int, n: int) -> QueryResult:
        """
        Benchmark query #x: Last N datasets for a specific vehicle.
        """
        raise NotImplementedError

    @abstractmethod
    def dashboard_speed_10m(self, vehicle_id: int, start_ts: datetime, end_ts: datetime) -> QueryResult:
        """
        Benchmark query #x: Speed in 10min buckets over a specific time frame.
        """
        raise NotImplementedError

    @abstractmethod
    def dashboard_speed_10m_multi(
        self,
        vehicle_ids: Sequence[int],
        start_ts: datetime,
        end_ts: datetime,
    ) -> QueryResult:
        """
        Benchmark query #x: Speed in 10min buckets over a specific time frame for multiple vehicles.
        """
        raise NotImplementedError

    @abstractmethod
    def insert_batch(self, batch: InsertBatch) -> QueryResult:
        """
        Insert a batch of data into the database.
        """
        raise NotImplementedError

    def create_new_vehicle(self) -> int:
        """
        Create a new vehicle entry in the database and return its ID.
        """
        raise NotImplementedError

    def create_new_job(self, vehicle_id: int) -> int:
        """
        Create a new job entry in the database and return its ID.
        """
        raise NotImplementedError

    def clean_data(self, vehicle_id: int, job_id: int) -> None:
        """
        Delete all the entries from the database which were made for the insert benchmark.
        """
        raise NotImplementedError
