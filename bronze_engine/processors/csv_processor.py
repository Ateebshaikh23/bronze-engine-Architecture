"""
CSV Processor

Responsibilities
----------------
1. Read CSV directly from MinIO
2. Strip trailing footnote/source rows (common in Figure.nz exports)
3. Batch rows before inserting
4. Load into bronze_csv

No SQL
No MinIO connection
"""

import io
import time
import pandas as pd

from bronze_engine.common.logger import get_logger

logger = get_logger(__name__)


class CSVProcessor:

    def __init__(self, postgres_loader):
        self.loader = postgres_loader

    # ==========================================================
    # Strip footnote / source rows
    # ==========================================================

    def _row_filled_ratio(self, row):
        non_empty = row.notna() & (row.astype(str).str.strip() != "")
        return non_empty.sum() / len(row)

    def _strip_footer_rows(self, dataframe, min_filled_ratio=0.3):
        """
        Figure.nz CSV exports often end with trailing rows like:
            ,,,,
            Source: Sport NZ Active NZ Survey,,,,

        These aren't data rows, so drop trailing rows where the
        fraction of non-empty cells is below `min_filled_ratio`.
        Only the TAIL is checked, so sparse data rows in the middle
        of a real table are left alone.
        """
        if dataframe.empty:
            return dataframe

        filled_ratio = dataframe.apply(self._row_filled_ratio, axis=1)

        last_valid_index = len(dataframe)
        for ratio in filled_ratio.iloc[::-1]:
            if ratio < min_filled_ratio:
                last_valid_index -= 1
            else:
                break

        if last_valid_index < len(dataframe):
            dropped = len(dataframe) - last_valid_index
            logger.info(f"Dropped {dropped} trailing footnote/empty row(s)")

        return dataframe.iloc[:last_valid_index]

    # ==========================================================
    # Process CSV
    # ==========================================================

    def process(
        self,
        stream,
        dataset_name,
        folder_name,
        file_name,
        minio_path,
        batch_size=5000,
    ):

        logger.info(f"Started processing : {file_name}")

        start_time = time.time()
        total_rows = 0

        try:

            dataframe = pd.read_csv(
                io.BytesIO(stream.read()),
                low_memory=False,
                dtype=str,
                encoding_errors="ignore",
            )

            dataframe.columns = [
                str(col).strip() for col in dataframe.columns
            ]

            dataframe = self._strip_footer_rows(dataframe)
            dataframe = dataframe.fillna("")

            for start in range(0, len(dataframe), batch_size):

                batch = dataframe.iloc[start:start + batch_size]

                self.loader.insert_dataframe(
                    dataframe=batch,
                    table_name="bronze_csv",
                    dataset_name=dataset_name,
                    folder_name=folder_name,
                    file_name=file_name,
                    minio_path=minio_path,
                )

                total_rows += len(batch)
                logger.info(f"{file_name} : {total_rows} rows loaded.")

            logger.info(f"{file_name} completed successfully.")
            logger.info(
                f"Execution Time : {round(time.time() - start_time, 2)} seconds"
            )

        except Exception:
            logger.exception(f"CSV Processing Failed : {file_name}")
            raise