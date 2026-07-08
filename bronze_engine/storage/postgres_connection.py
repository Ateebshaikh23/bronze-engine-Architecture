"""
PostgreSQL Connection Manager

Responsibilities
----------------
1. Create SQLAlchemy Engine
2. Manage Connection Pool
3. Provide database connections
4. Test database connectivity

This module DOES NOT:
- Create tables
- Insert data
- Perform transformations

Those responsibilities belong to postgres_schema.py
and postgres_loader.py
"""

from sqlalchemy import create_engine, text
from sqlalchemy.engine import Engine
from sqlalchemy.exc import SQLAlchemyError

from bronze_engine.common.logger import get_logger

logger = get_logger(__name__)


class PostgreSQLConnection:

    def __init__(
        self,
        host: str,
        port: int,
        database: str,
        username: str,
        password: str,
    ):

        self.connection_string = (
            f"postgresql+psycopg2://"
            f"{username}:{password}"
            f"@{host}:{port}"
            f"/{database}"
        )

        self.engine = create_engine(
            self.connection_string,

            # Connection Pool
            pool_size=10,
            max_overflow=20,
            pool_timeout=30,
            pool_recycle=1800,

            future=True,

            echo=False
        )

    # ----------------------------------------------------

    def get_engine(self) -> Engine:
        """
        Returns SQLAlchemy Engine
        """
        return self.engine

    # ----------------------------------------------------

    def get_connection(self):
        """
        Returns active database connection.
        """

        return self.engine.connect()

    # ----------------------------------------------------

    def test_connection(self):

        try:

            with self.engine.connect() as conn:

                conn.execute(
                    text("SELECT 1")
                )

            logger.info(
                "PostgreSQL connection successful."
            )

            return True

        except SQLAlchemyError as e:

            logger.error(e)

            return False

    # ----------------------------------------------------

    def close(self):

        self.engine.dispose()

        logger.info(
            "PostgreSQL connections closed."
        )


# ==========================================================
# Test
# ==========================================================

if __name__ == "__main__":

    db = PostgreSQLConnection(

        host="localhost",
        port=5433,
        database="sport_nz_db",
        username="postgres",
        password="postgres"

    )

    db.test_connection()

    db.close()