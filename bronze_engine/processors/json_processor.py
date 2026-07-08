"""
JSON Processor

Responsibilities
----------------
1. Read JSON directly from MinIO
2. Preserve JSON exactly as received
3. Load raw JSON into bronze_json

No SQL
No MinIO connection
No Transformations
"""

import json
import pandas as pd

from bronze_engine.common.logger import get_logger

logger = get_logger(__name__)


class JSONProcessor:

    def __init__(self, postgres_loader):

        self.loader = postgres_loader

    # ==========================================================
    # Process JSON
    # ==========================================================

    def process(
        self,
        stream,
        dataset_name,
        folder_name,
        file_name,
        minio_path,
    ):

        logger.info(f"Started processing : {file_name}")

        try:

            json_data = json.load(stream)

            # --------------------------------------------------
            # JSON Array
            # --------------------------------------------------

            if isinstance(json_data, list):

                dataframe = pd.json_normalize(json_data)

            # --------------------------------------------------
            # JSON Object
            # --------------------------------------------------

            elif isinstance(json_data, dict):

                dataframe = pd.json_normalize([json_data])

            else:

                raise ValueError(
                    f"Unsupported JSON structure : {type(json_data)}"
                )

            dataframe = dataframe.fillna("").astype(str)

            self.loader.insert_dataframe(

                dataframe=dataframe,

                table_name="bronze_json",

                dataset_name=dataset_name,

                folder_name=folder_name,

                file_name=file_name,

                minio_path=minio_path

            )

            logger.info(
                f"{file_name} processed successfully. "
                f"Rows Loaded : {len(dataframe)}"
            )

        except Exception as e:

            logger.error(
                f"Failed processing {file_name} : {e}"
            )

            raise