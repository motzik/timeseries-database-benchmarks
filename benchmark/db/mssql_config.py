import os
from dataclasses import dataclass


@dataclass
class MSSQLConfig:
    host: str
    password: str
    port: int = 1433
    database: str = ""
    user: str = "sa"
    driver: str = "ODBC Driver 17 for SQL Server"
    encrypt: bool = False

    @staticmethod
    def from_env(prefix: str) -> "MSSQLConfig":
        return MSSQLConfig(
            host=os.getenv(f"{prefix}_HOST", "localhost"),
            port=int(os.getenv(f"{prefix}_PORT", "1433")),
            database=os.getenv(f"{prefix}_DATABASE", "change_me"),
            user=os.getenv(f"{prefix}_USER", "sa"),
            password=os.getenv(f"{prefix}_PASSWORD", ""),
            driver=os.environ.get(
                f"{prefix}_DRIVER",
                "ODBC Driver 18 for SQL Server",
            ),
            encrypt=os.environ.get(f"{prefix}_ENCRYPT", "false").lower() == "true",
        )
