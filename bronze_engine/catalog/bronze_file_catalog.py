"""
Bronze File Catalog

Tracks every file processed by the Bronze Layer.

Responsibilities
----------------
1. Register processing start
2. Register processing success
3. Register processing failure
4. Check if file already processed
"""

from datetime import datetime

from sqlalchemy import text

from bronze_engine.common.logger import get_logger

logger = get_logger(__name__)


class BronzeFileCatalog:

    def __init__(self, postgres_connection):

        self.engine = postgres_connection.get_engine()

    # =====================================================
    # Check Existing File
    # =====================================================

    def is_processed(self, object_name, etag):

        query = text("""

            SELECT id

            FROM bronze.bronze_file_catalog

            WHERE object_name = :object_name

            AND etag = :etag

            AND status = 'SUCCESS'

            LIMIT 1

        """)

        with self.engine.begin() as conn:

            result = conn.execute(

                query,

                {

                    "object_name": object_name,

                    "etag": etag

                }

            ).fetchone()

        return result is not None

    # =====================================================
    # Register Processing Start
    # =====================================================

    def register_start(

        self,

        bucket_name,

        object_name,

        file_name,

        folder_name,

        extension,

        file_size,

        etag,

        last_modified

    ):

        query = text("""

            INSERT INTO bronze.bronze_file_catalog(

                bucket_name,

                object_name,

                file_name,

                folder_name,

                extension,

                file_size,

                etag,

                last_modified,

                status,

                processing_started

            )

            VALUES(

                :bucket_name,

                :object_name,

                :file_name,

                :folder_name,

                :extension,

                :file_size,

                :etag,

                :last_modified,

                'PROCESSING',

                :processing_started

            )

            RETURNING id

        """)

        with self.engine.begin() as conn:

            result = conn.execute(

                query,

                {

                    "bucket_name": bucket_name,

                    "object_name": object_name,

                    "file_name": file_name,

                    "folder_name": folder_name,

                    "extension": extension,

                    "file_size": file_size,

                    "etag": etag,

                    "last_modified": last_modified,

                    "processing_started": datetime.utcnow()

                }

            )

            catalog_id = result.scalar()

        logger.info(f"Catalog Start : {file_name}")

        return catalog_id

    # =====================================================
    # Register Success
    # =====================================================

    def register_success(

        self,

        catalog_id,

        rows_loaded

    ):

        query = text("""

            UPDATE bronze.bronze_file_catalog

            SET

                status='SUCCESS',

                rows_loaded=:rows_loaded,

                processing_completed=:processing_completed

            WHERE id=:id

        """)

        with self.engine.begin() as conn:

            conn.execute(

                query,

                {

                    "id": catalog_id,

                    "rows_loaded": rows_loaded,

                    "processing_completed": datetime.utcnow()

                }

            )

        logger.info(f"Catalog Success : {catalog_id}")

    # =====================================================
    # Register Failure
    # =====================================================

    def register_failure(

        self,

        catalog_id,

        error_message

    ):

        query = text("""

            UPDATE bronze.bronze_file_catalog

            SET

                status='FAILED',

                error_message=:error_message,

                processing_completed=:processing_completed

            WHERE id=:id

        """)

        with self.engine.begin() as conn:

            conn.execute(

                query,

                {

                    "id": catalog_id,

                    "error_message": str(error_message),

                    "processing_completed": datetime.utcnow()

                }

            )

        logger.error(f"Catalog Failed : {catalog_id}")