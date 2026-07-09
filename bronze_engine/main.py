import os

from dotenv import load_dotenv

from bronze_engine.common.logger import get_logger
from bronze_engine.common.file_catalog import FileCatalog

from bronze_engine.storage.minio_client import MinIOClient
from bronze_engine.storage.postgres_connection import PostgreSQLConnection
from bronze_engine.storage.postgres_schema import PostgreSQLSchema
from bronze_engine.storage.postgres_loader import PostgreSQLLoader

from bronze_engine.processors.processor_factory import ProcessorFactory
from bronze_engine.connectors.minio_connector import MinIOConnector

load_dotenv()
logger = get_logger(__name__)


def get_env(variable):
    value = os.getenv(variable)
    if not value:
        raise ValueError(f"Environment Variable Missing : {variable}")
    return value


def main():

    logger.info("=" * 70)
    logger.info("Starting Bronze Layer")
    logger.info("=" * 70)

    postgres = PostgreSQLConnection(
        host=get_env("POSTGRES_HOST"),
        port=int(get_env("POSTGRES_PORT")),
        database=get_env("POSTGRES_DATABASE"),
        username=get_env("POSTGRES_USER"),
        password=get_env("POSTGRES_PASSWORD"),
    )

    if not postgres.test_connection():
        raise Exception("Unable to connect PostgreSQL")

    schema = PostgreSQLSchema(postgres.get_engine())
    schema.create_schema()
    schema.create_all_tables()

    loader = PostgreSQLLoader(connection=postgres, schema_manager=schema)
    file_catalog = FileCatalog(postgres.get_engine())

    minio = MinIOClient(
        endpoint=get_env("MINIO_ENDPOINT"),
        access_key=get_env("MINIO_ACCESS_KEY"),
        secret_key=get_env("MINIO_SECRET_KEY"),
        secure=get_env("MINIO_SECURE").lower() == "true",
    )

    if not minio.bucket_exists(get_env("MINIO_BUCKET")):
        raise Exception("MinIO Bucket Not Found")

    factory = ProcessorFactory(postgres_loader=loader)

    connector = MinIOConnector(
        minio_client=minio,
        processor_factory=factory,
        bucket_name=get_env("MINIO_BUCKET"),
        file_catalog=file_catalog,
    )

    connector.process_bucket()

    logger.info("=" * 70)
    logger.info("Bronze Layer Completed Successfully")
    logger.info("=" * 70)

    postgres.close()


if __name__ == "__main__":
    main()