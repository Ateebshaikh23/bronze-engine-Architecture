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

from sqlalchemy import text

from bronze_engine.common.logger import get_logger
from bronze_engine.storage.postgres_schema import PostgreSQLSchema

logger = get_logger(__name__)


class PostgreSQLLoader:

    def __init__(
        self,
        connection,
        schema_manager: PostgreSQLSchema
    ):

        self.connection = connection
        self.engine = connection.get_engine()
        self.schema_manager = schema_manager

    # ==========================================================
    # Prepare DataFrame
    # ==========================================================

    def prepare_dataframe(
        self,
        dataframe: pd.DataFrame
    ) -> pd.DataFrame:

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

        minio_path: str

    ):

        dataframe = self.prepare_dataframe(dataframe)

        dataframe["dataset_name"] = dataset_name

        dataframe["folder_name"] = folder_name

        dataframe["file_name"] = file_name

        dataframe["minio_path"] = minio_path

        self.schema_manager.add_missing_columns(

            table_name,

            dataframe.columns.tolist()

        )

        columns = dataframe.columns.tolist()

        quoted_columns = [

            f'"{column}"'

            for column in columns

        ]

        placeholders = [

            f":{column}"

            for column in columns

        ]

        sql = f"""

        INSERT INTO bronze.{table_name}

        (

        {",".join(quoted_columns)}

        )

        VALUES

        (

        {",".join(placeholders)}

        )

        """

        records = dataframe.to_dict(

            orient="records"

        )

        with self.engine.begin() as conn:

            conn.execute(

                text(sql),

                records

            )

        logger.info(

            f"{len(records)} rows inserted into {table_name}"

        )