from abc import ABC, abstractmethod
from typing import Any, Iterable, Optional, Sequence
from dataclasses import dataclass
from datetime import datetime

@dataclass(frozen=True)
class QueryResult:
    """Result metadata returned by database adapters"""
    row_count: int

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
