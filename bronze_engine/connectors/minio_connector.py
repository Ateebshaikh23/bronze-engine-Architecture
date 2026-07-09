"""
MinIO Connector

Responsibilities
----------------
1. Read files from MinIO
2. Skip files already loaded (via FileCatalog)
3. Detect processor
4. Send content to processor
5. Record the outcome in FileCatalog
6. Continue processing if one file fails

No SQL
No Transformations
"""

from pathlib import PurePosixPath
import io

from bronze_engine.common.logger import get_logger
from bronze_engine.common.file_catalog import FileCatalog

logger = get_logger(__name__)


class MinIOConnector:

    def __init__(self, minio_client, processor_factory, bucket_name, file_catalog: FileCatalog):
        self.minio = minio_client
        self.processor_factory = processor_factory
        self.bucket_name = bucket_name
        self.file_catalog = file_catalog

    def process_bucket(self):

        logger.info(f"Scanning bucket : {self.bucket_name}")

        total_files = 0
        processed_files = 0
        duplicate_files = 0
        skipped_files = 0
        failed_files = 0

        objects = self.minio.list_objects(bucket_name=self.bucket_name, recursive=True)

        for obj in objects:

            object_name = obj.object_name
            if object_name.endswith("/"):
                continue

            total_files += 1

            path = PurePosixPath(object_name)
            file_name = path.name
            extension = path.suffix.lower()
            folder_name = path.parent.name if path.parent.name else "root"
            dataset_name = folder_name

            processor = self.processor_factory.get_processor(file_name)

            if processor is None:
                skipped_files += 1
                logger.warning(f"Skipping unsupported file : {object_name}")
                continue

            response = None
            try:
                response = self.minio.get_stream(self.bucket_name, object_name)
                data = response.read()
            finally:
                if response:
                    response.close()
                    response.release_conn()

            checksum = FileCatalog.compute_checksum(data)

            if self.file_catalog.is_duplicate(checksum):
                duplicate_files += 1
                logger.info(f"Already ingested, skipping : {object_name}")
                continue

            logger.info(f"Processing : {object_name}")

            try:
                processor.process(
                    stream=io.BytesIO(data),
                    dataset_name=dataset_name,
                    folder_name=folder_name,
                    file_name=file_name,
                    minio_path=object_name,
                )

                processed_files += 1

                self.file_catalog.record(
                    checksum=checksum, minio_path=object_name,
                    dataset_name=dataset_name, folder_name=folder_name,
                    file_name=file_name, extension=extension,
                    size_bytes=len(data), status="success",
                )

            except Exception as e:
                failed_files += 1
                logger.exception(f"Failed processing {object_name}: {e}")

                self.file_catalog.record(
                    checksum=checksum, minio_path=object_name,
                    dataset_name=dataset_name, folder_name=folder_name,
                    file_name=file_name, extension=extension,
                    size_bytes=len(data), status="failed",
                    error_message=str(e)[:500],
                )

        logger.info("=" * 60)
        logger.info("Bronze Ingestion Summary")
        logger.info("=" * 60)
        logger.info(f"Total Files      : {total_files}")
        logger.info(f"Processed Files  : {processed_files}")
        logger.info(f"Duplicate Files  : {duplicate_files}")
        logger.info(f"Skipped Files    : {skipped_files}")
        logger.info(f"Failed Files     : {failed_files}")
        logger.info("=" * 60)