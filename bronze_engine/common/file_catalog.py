"""
File Catalog

Tracks every file the Bronze layer has ingested, keyed by a SHA-256
checksum of its content. Used to:
  1. Skip files already successfully loaded (even if renamed/moved).
  2. Retry files that previously failed.
  3. Keep provenance metadata (path, size, extension, when).

Works directly on bytes already pulled from MinIO — it does not
read from or expect a local filesystem path.
"""

import hashlib

from sqlalchemy import text

from bronze_engine.common.logger import get_logger

logger = get_logger(__name__)


class FileCatalog:

    def __init__(self, engine):
        self.engine = engine

    # ==========================================================
    # Checksum
    # ==========================================================

    @staticmethod
    def compute_checksum(data: bytes) -> str:
        return hashlib.sha256(data).hexdigest()

    # ==========================================================
    # Lookup
    # ==========================================================

    def is_duplicate(self, checksum: str) -> bool:
        """
        True only if this exact content already loaded successfully.
        Files recorded as 'failed' are NOT treated as duplicates,
        so they get retried on the next run.
        """

        query = text(
            """
            SELECT 1 FROM bronze.bronze_file_catalog
            WHERE checksum = :checksum AND status = 'success'
            LIMIT 1;
            """
        )

        with self.engine.connect() as conn:
            result = conn.execute(query, {"checksum": checksum}).first()

        return result is not None

    # ==========================================================
    # Record
    # ==========================================================

    def record(
        self,
        checksum: str,
        minio_path: str,
        dataset_name: str,
        folder_name: str,
        file_name: str,
        extension: str,
        size_bytes: int,
        status: str,
        error_message: str = None,
    ):
        """
        Insert or update the catalog entry for this checksum.
        ON CONFLICT lets a previously 'failed' file flip to
        'success' once it's fixed and reprocessed.
        """

        query = text(
            """
            INSERT INTO bronze.bronze_file_catalog
                (checksum, minio_path, dataset_name, folder_name,
                 file_name, extension, size_bytes, status,
                 error_message, ingestion_timestamp)
            VALUES
                (:checksum, :minio_path, :dataset_name, :folder_name,
                 :file_name, :extension, :size_bytes, :status,
                 :error_message, NOW())
            ON CONFLICT (checksum) DO UPDATE SET
                minio_path = EXCLUDED.minio_path,
                dataset_name = EXCLUDED.dataset_name,
                folder_name = EXCLUDED.folder_name,
                file_name = EXCLUDED.file_name,
                extension = EXCLUDED.extension,
                size_bytes = EXCLUDED.size_bytes,
                status = EXCLUDED.status,
                error_message = EXCLUDED.error_message,
                ingestion_timestamp = NOW();
            """
        )

        with self.engine.begin() as conn:
            conn.execute(
                query,
                {
                    "checksum": checksum,
                    "minio_path": minio_path,
                    "dataset_name": dataset_name,
                    "folder_name": folder_name,
                    "file_name": file_name,
                    "extension": extension,
                    "size_bytes": size_bytes,
                    "status": status,
                    "error_message": error_message,
                },
            )

        logger.info(f"Catalog recorded : {file_name} [{status}]")