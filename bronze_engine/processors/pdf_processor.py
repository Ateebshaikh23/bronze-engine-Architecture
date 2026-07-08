"""
PDF Processor

Responsibilities
----------------
1. Read PDF directly from MinIO
2. Extract metadata
3. Extract page text
4. Load into bronze_pdf

No SQL
No MinIO connection
"""

from io import BytesIO

import fitz
import pandas as pd

from bronze_engine.common.logger import get_logger

logger = get_logger(__name__)


class PDFProcessor:

    def __init__(self, postgres_loader):

        self.loader = postgres_loader

    # ==========================================================
    # Process PDF
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

        pdf = fitz.open(stream=stream.read(), filetype="pdf")

        metadata = pdf.metadata

        rows = []

        try:

            for page_number, page in enumerate(pdf, start=1):

                rows.append(

                    {

                        "page_number": str(page_number),

                        "page_text": page.get_text(),

                        "author": metadata.get("author", ""),

                        "title": metadata.get("title", ""),

                        "subject": metadata.get("subject", ""),

                        "creator": metadata.get("creator", ""),

                        "producer": metadata.get("producer", ""),

                        "creation_date": metadata.get("creationDate", ""),

                        "modification_date": metadata.get("modDate", ""),

                    }

                )

            dataframe = pd.DataFrame(rows)

            dataframe = dataframe.fillna("").astype(str)

            self.loader.insert_dataframe(

                dataframe=dataframe,

                table_name="bronze_pdf",

                dataset_name=dataset_name,

                folder_name=folder_name,

                file_name=file_name,

                minio_path=minio_path,

            )

            logger.info(

                f"{file_name} processed successfully."

            )

        finally:

            pdf.close()