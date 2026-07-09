"""
PostgreSQL Schema Manager

Creates Bronze Schema
Creates Bronze Tables
Adds Missing Columns Automatically

NO DATA INSERTS
"""

import hashlib
import re

from sqlalchemy import inspect, text

from bronze_engine.common.logger import get_logger

logger = get_logger(__name__)

MAX_IDENTIFIER_LENGTH = 63


class PostgreSQLSchema:

    def __init__(self, engine):
        self.engine = engine
        self.schema = "bronze"
        self._column_cache = {}

    def create_schema(self):
        with self.engine.begin() as conn:
            conn.execute(text("CREATE SCHEMA IF NOT EXISTS bronze;"))

    def create_base_table(self, table_name):
        query = f"""
        CREATE TABLE IF NOT EXISTS bronze.{table_name} (
            id BIGSERIAL PRIMARY KEY,
            dataset_name TEXT,
            folder_name TEXT,
            file_name TEXT,
            minio_path TEXT,
            ingestion_timestamp TIMESTAMP DEFAULT NOW()
        );
        """
        with self.engine.begin() as conn:
            conn.execute(text(query))
        logger.info(f"{table_name} ready.")

    # File catalog gets its own shape (checksum, status, size)
    # instead of the generic data-table shape above.
    def create_file_catalog_table(self):
        query = """
        CREATE TABLE IF NOT EXISTS bronze.bronze_file_catalog (
            id BIGSERIAL PRIMARY KEY,
            checksum TEXT NOT NULL,
            minio_path TEXT NOT NULL,
            dataset_name TEXT,
            folder_name TEXT,
            file_name TEXT,
            extension TEXT,
            size_bytes BIGINT,
            status TEXT NOT NULL DEFAULT 'success',
            error_message TEXT,
            ingestion_timestamp TIMESTAMP DEFAULT NOW()
        );
        """
        index_query = """
        CREATE UNIQUE INDEX IF NOT EXISTS bronze_file_catalog_checksum_idx
        ON bronze.bronze_file_catalog (checksum);
        """
        with self.engine.begin() as conn:
            conn.execute(text(query))
            conn.execute(text(index_query))
        logger.info("bronze_file_catalog ready.")

    def create_all_tables(self):
        tables = [
            "bronze_csv", "bronze_excel", "bronze_pdf",
            "bronze_json", "bronze_xml", "bronze_zip", "bronze_parquet",
        ]
        for table in tables:
            self.create_base_table(table)

        self.create_file_catalog_table()

    def existing_columns(self, table_name):
        if table_name not in self._column_cache:
            inspector = inspect(self.engine)
            columns = inspector.get_columns(table_name, schema=self.schema)
            self._column_cache[table_name] = {c["name"] for c in columns}
        return self._column_cache[table_name]

    def clean_column(self, column):
        column = str(column).strip().lower()
        column = re.sub(r"[ /\\\-]", "_", column)
        column = re.sub(r"[^a-zA-Z0-9_]", "", column)
        column = re.sub(r"_+", "_", column).strip("_")

        if not column:
            column = "unnamed_column"

        if len(column) > MAX_IDENTIFIER_LENGTH:
            suffix = hashlib.md5(column.encode()).hexdigest()[:8]
            column = f"{column[:MAX_IDENTIFIER_LENGTH - 9]}_{suffix}"

        return column

    def add_missing_columns(self, table_name, columns):
        existing = self.existing_columns(table_name)
        to_add = []

        for column in columns:
            cleaned = self.clean_column(column)
            if cleaned not in existing and cleaned not in to_add:
                to_add.append(cleaned)

        if not to_add:
            return

        with self.engine.begin() as conn:
            for column in to_add:
                conn.execute(
                    text(f'ALTER TABLE bronze.{table_name} ADD COLUMN "{column}" TEXT;')
                )
                logger.info(f"Added {column} to {table_name}")

        existing.update(to_add)