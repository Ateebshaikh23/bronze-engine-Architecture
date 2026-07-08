from io import BytesIO
from typing import Generator

from minio import Minio
from minio.error import S3Error

from bronze_engine.common.logger import get_logger

logger = get_logger(__name__)


class MinIOClient:

    def __init__(
        self,
        endpoint: str,
        access_key: str,
        secret_key: str,
        secure: bool = False,
    ):

        self.client = Minio(
            endpoint=endpoint,
            access_key=access_key,
            secret_key=secret_key,
            secure=secure,
        )

    # ----------------------------------------------------
    # Bucket Operations
    # ----------------------------------------------------

    def create_bucket(self, bucket_name: str):

        if not self.client.bucket_exists(bucket_name):
            self.client.make_bucket(bucket_name)
            logger.info(f"Bucket created : {bucket_name}")

    def bucket_exists(self, bucket_name: str):

        return self.client.bucket_exists(bucket_name)

    # ----------------------------------------------------
    # Object Operations
    # ----------------------------------------------------

    def list_objects(
        self,
        bucket_name: str,
        prefix: str = "",
        recursive: bool = True,
    ) -> Generator:

        return self.client.list_objects(
            bucket_name=bucket_name,
            prefix=prefix,
            recursive=recursive,
        )

    def object_exists(
        self,
        bucket_name: str,
        object_name: str,
    ):

        try:

            self.client.stat_object(
                bucket_name,
                object_name,
            )

            return True

        except S3Error:

            return False

    def stat_object(
        self,
        bucket_name: str,
        object_name: str,
    ):

        return self.client.stat_object(
            bucket_name,
            object_name,
        )

    # ----------------------------------------------------
    # Download
    # ----------------------------------------------------

    def get_stream(
        self,
        bucket_name: str,
        object_name: str,
    ):

        return self.client.get_object(
            bucket_name,
            object_name,
        )

    def download_bytes(
        self,
        bucket_name: str,
        object_name: str,
    ) -> BytesIO:

        response = self.client.get_object(
            bucket_name,
            object_name,
        )

        try:

            return BytesIO(response.read())

        finally:

            response.close()
            response.release_conn()

    # ----------------------------------------------------
    # Upload
    # ----------------------------------------------------

    def upload_file(
        self,
        bucket_name: str,
        object_name: str,
        file_path: str,
    ):

        self.client.fput_object(
            bucket_name,
            object_name,
            file_path,
        )

        logger.info(f"{object_name} uploaded successfully.")

    # ----------------------------------------------------
    # Delete
    # ----------------------------------------------------

    def delete_object(
        self,
        bucket_name: str,
        object_name: str,
    ):

        self.client.remove_object(
            bucket_name,
            object_name,
        )

        logger.info(f"{object_name} deleted successfully.")