"""
PostgreSQL Schema Manager

Creates Bronze Schema
Creates Bronze Tables
Adds Missing Columns Automatically

NO DATA INSERTS
"""

import re

from sqlalchemy import inspect, text

from bronze_engine.common.logger import get_logger

logger = get_logger(__name__)


class PostgreSQLSchema:

    def __init__(self, engine):

        self.engine = engine

        self.schema = "bronze"

    # =========================================================

    def create_schema(self):

        with self.engine.begin() as conn:

            conn.execute(

                text(
                    "CREATE SCHEMA IF NOT EXISTS bronze;"
                )

            )

    # =========================================================

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

    # =========================================================

    def create_all_tables(self):

        tables = [

            "bronze_csv",

            "bronze_excel",

            "bronze_pdf",

            "bronze_json",

            "bronze_xml",

            "bronze_zip",

            "bronze_parquet",

            "bronze_file_catalog"

        ]

        for table in tables:

            self.create_base_table(table)

    # =========================================================

    def existing_columns(self, table_name):

        inspector = inspect(self.engine)

        columns = inspector.get_columns(

            table_name,

            schema=self.schema

        )

        return [

            column["name"]

            for column in columns

        ]

    # =========================================================

    def clean_column(self, column):

        column = column.strip()

        column = column.lower()

        column = re.sub(r"[ /\\\-]", "_", column)

        column = re.sub(r"[^a-zA-Z0-9_]", "", column)

        return column

    # =========================================================

    def add_missing_columns(

        self,

        table_name,

        columns

    ):

        existing = self.existing_columns(

            table_name

        )

        with self.engine.begin() as conn:

            for column in columns:

                column = self.clean_column(column)

                if column not in existing:

                    conn.execute(

                        text(

                            f"""

                            ALTER TABLE bronze.{table_name}

                            ADD COLUMN "{column}" TEXT;

                            """

                        )

                    )

                    logger.info(

                        f"Added {column} to {table_name}"

                    )