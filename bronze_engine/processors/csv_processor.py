import io
import time
import pandas as pd

from bronze_engine.common.logger import get_logger

logger = get_logger(__name__)


class CSVProcessor:

    def __init__(self, postgres_loader):
        self.loader = postgres_loader

    def process(
        self,
        minio_client,
        bucket_name,
        object_name,
    ):

        logger.info(f"Started processing : {object_name.split('/')[-1]}")

        start_time = time.time()

        response = None
        total_rows = 0

        try:

            # --------------------------------------------------
            # Get stream from MinIO
            # --------------------------------------------------

            response = minio_client.client.get_object(
                bucket_name,
                object_name
            )

            # --------------------------------------------------
            # Read stream into memory
            # (avoids closed stream issue)
            # --------------------------------------------------

            data = response.read()

            stream = io.BytesIO(data)

            # --------------------------------------------------
            # Read CSV in chunks
            # --------------------------------------------------

            reader = pd.read_csv(

                stream,

                chunksize=50000,

                low_memory=False,

                dtype=str,

                encoding_errors="ignore"

            )

            # --------------------------------------------------
            # Process every chunk
            # --------------------------------------------------

            for chunk in reader:

                chunk = chunk.fillna("")

                chunk.columns = [

                    str(col).strip()

                    for col in chunk.columns

                ]

                self.loader.load_dataframe(

                    dataframe=chunk,

                    table_name="bronze_csv"

                )

                total_rows += len(chunk)

                logger.info(

                    f"{object_name} : {total_rows} rows loaded."

                )

            logger.info(

                f"{object_name} completed successfully."

            )

            logger.info(

                f"Execution Time : {round(time.time()-start_time,2)} seconds"

            )

        except Exception:

            logger.exception(

                f"CSV Processing Failed : {object_name}"

            )

            raise

        finally:

            if response:

                response.close()

                response.release_conn()