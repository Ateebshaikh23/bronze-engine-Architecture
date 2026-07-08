"""
MinIO Connector

Responsibilities
----------------
1. Read files from MinIO
2. Detect processor
3. Send stream to processor
4. Continue processing if one file fails

No SQL
No Transformations
"""

from pathlib import PurePosixPath

from bronze_engine.common.logger import get_logger

logger = get_logger(__name__)


class MinIOConnector:

    def __init__(
        self,
        minio_client,
        processor_factory,
        bucket_name,
    ):

        self.minio = minio_client
        self.processor_factory = processor_factory
        self.bucket_name = bucket_name

    # ==========================================================
    # Process Bucket
    # ==========================================================

    def process_bucket(self):

        logger.info(f"Scanning bucket : {self.bucket_name}")

        total_files = 0
        processed_files = 0
        skipped_files = 0
        failed_files = 0

        objects = self.minio.list_objects(
            bucket_name=self.bucket_name,
            recursive=True,
        )

        for obj in objects:

            object_name = obj.object_name

            # Skip folders
            if object_name.endswith("/"):
                continue

            total_files += 1

            path = PurePosixPath(object_name)

            file_name = path.name

            folder_name = (
                path.parent.name
                if path.parent.name
                else "root"
            )

            dataset_name = folder_name

            processor = self.processor_factory.get_processor(
                file_name
            )

            if processor is None:

                skipped_files += 1

                logger.warning(
                    f"Skipping unsupported file : {object_name}"
                )

                continue

            logger.info(f"Processing : {object_name}")

            response = None

            try:

                response = self.minio.get_stream(
                    self.bucket_name,
                    object_name,
                )

                processor.process(

                    stream=response,

                    dataset_name=dataset_name,

                    folder_name=folder_name,

                    file_name=file_name,

                    minio_path=object_name,

                )

                processed_files += 1

            except Exception as e:

                failed_files += 1

                logger.exception(
                    f"Failed processing {object_name}: {e}"
                )

            finally:

                if response:

                    response.close()
                    response.release_conn()

        logger.info("=" * 60)
        logger.info("Bronze Ingestion Summary")
        logger.info("=" * 60)
        logger.info(f"Total Files      : {total_files}")
        logger.info(f"Processed Files  : {processed_files}")
        logger.info(f"Skipped Files    : {skipped_files}")
        logger.info(f"Failed Files     : {failed_files}")
        logger.info("=" * 60)