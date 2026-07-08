from pathlib import Path
import hashlib

from bronze_engine.common.logger import get_logger

logger = get_logger(__name__)


class FileCatalog:
    """
    Stores metadata about every file processed
    in the Bronze layer.
    """

    def __init__(self, file_path: str):
        self.file_path = Path(file_path)

        if not self.file_path.exists():
            raise FileNotFoundError(self.file_path)

    def file_name(self):
        return self.file_path.name

    def extension(self):
        return self.file_path.suffix.lower()

    def size(self):
        return self.file_path.stat().st_size

    def checksum(self):
        md5 = hashlib.md5()

        with open(self.file_path, "rb") as f:
            for chunk in iter(lambda: f.read(4096), b""):
                md5.update(chunk)

        return md5.hexdigest()

    def metadata(self):

        meta = {
            "file_name": self.file_name(),
            "extension": self.extension(),
            "size_bytes": self.size(),
            "checksum": self.checksum(),
            "absolute_path": str(self.file_path.resolve())
        }

        logger.info(f"Catalog created for {self.file_name()}")

        return meta


if __name__ == "__main__":

    sample = FileCatalog("README.md")

    print(sample.metadata())