"""
PostgreSQL Loader

Responsibilities
----------------
1. Bulk insert data
2. Auto add missing columns
3. Transaction handling

NO SCHEMA CREATION
NO FILE READING
"""

import pandas as pd
import psycopg2.extras as pg_extras

from bronze_engine.common.logger import get_logger
from bronze_engine.storage.postgres_schema import PostgreSQLSchema

logger = get_logger(__name__)


class PostgreSQLLoader:

    def __init__(self, connection, schema_manager: PostgreSQLSchema):
        self.connection = connection
        self.engine = connection.get_engine()
        self.schema_manager = schema_manager

    # ==========================================================
    # Prepare DataFrame
    # ==========================================================

    def prepare_dataframe(self, dataframe: pd.DataFrame) -> pd.DataFrame:
        dataframe = dataframe.copy()
        dataframe.columns = [
            self.schema_manager.clean_column(column)
            for column in dataframe.columns
        ]
        dataframe = dataframe.astype(str)
        dataframe = dataframe.fillna("")
        return dataframe

    # ==========================================================
    # Bulk Insert
    # ==========================================================

    def insert_dataframe(
        self,
        dataframe: pd.DataFrame,
        table_name: str,
        dataset_name: str,
        folder_name: str,
        file_name: str,
        minio_path: str,
    ):

        if dataframe.empty:
            return

        dataframe = self.prepare_dataframe(dataframe)
        dataframe["dataset_name"] = dataset_name
        dataframe["folder_name"] = folder_name
        dataframe["file_name"] = file_name
        dataframe["minio_path"] = minio_path

        self.schema_manager.add_missing_columns(
            table_name,
            dataframe.columns.tolist(),
        )

        # Re-apply clean_column here too — add_missing_columns cleans
        # names for DDL, but the dataframe's own column labels are
        # still the raw/original ones and must match exactly.
        dataframe.columns = [
            self.schema_manager.clean_column(column)
            for column in dataframe.columns
        ]

        columns = dataframe.columns.tolist()
        quoted_columns = ", ".join(f'"{column}"' for column in columns)
        insert_sql = f'INSERT INTO bronze.{table_name} ({quoted_columns}) VALUES %s'

        records = [tuple(row) for row in dataframe.itertuples(index=False, name=None)]

        raw_conn = self.engine.raw_connection()
        try:
            with raw_conn.cursor() as cursor:
                pg_extras.execute_values(
                    cursor,
                    insert_sql,
                    records,
                    page_size=1000,
                )
            raw_conn.commit()
        except Exception:
            raw_conn.rollback()
            raise
        finally:
            raw_conn.close()

        logger.info(f"{len(records)} rows inserted into {table_name}")