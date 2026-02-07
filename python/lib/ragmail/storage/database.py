"""LanceDB database connection management."""

from pathlib import Path

import lancedb


class Database:
    """LanceDB database connection wrapper."""

    def __init__(self, path: Path | str):
        """Initialize database connection.

        Args:
            path: Path to the LanceDB database directory
        """
        self.path = Path(path)
        self._db = lancedb.connect(str(self.path))

    @property
    def db(self) -> lancedb.DBConnection:
        """Get the underlying database connection."""
        return self._db

    def table_names(self) -> list[str]:
        """List all table names in the database."""
        result = self._db.list_tables()
        if hasattr(result, 'tables'):
            return result.tables
        return list(result)

    def table_exists(self, name: str) -> bool:
        """Check if a table exists."""
        return name in self.table_names()

    def drop_table(self, name: str) -> None:
        """Drop a table if it exists."""
        if self.table_exists(name):
            self._db.drop_table(name)

    def close(self) -> None:
        """Close database connection."""
        pass
