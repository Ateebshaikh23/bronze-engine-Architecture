"""
Processor Factory

Responsibilities
----------------
1. Detect processor from file extension.
2. Return the correct processor.
3. Keep the Bronze pipeline independent of file types.

Example
-------
processor = factory.get_processor("sports.csv")
processor.process(...)
"""

from pathlib import Path

from bronze_engine.common.logger import get_logger

from bronze_engine.processors.csv_processor import CSVProcessor
from bronze_engine.processors.excel_processor import ExcelProcessor
from bronze_engine.processors.pdf_processor import PDFProcessor
from bronze_engine.processors.json_processor import JSONProcessor

logger = get_logger(__name__)


class ProcessorFactory:

    def __init__(self, postgres_loader):

        self.postgres_loader = postgres_loader

        # Register all supported processors here
        self.processors = {

            ".csv": CSVProcessor(self.postgres_loader),

            ".xlsx": ExcelProcessor(self.postgres_loader),

            ".xls": ExcelProcessor(self.postgres_loader),

            ".pdf": PDFProcessor(self.postgres_loader),

            ".json": JSONProcessor(self.postgres_loader),

        }

    # =====================================================
    # Get Processor
    # =====================================================

    def get_processor(self, file_name: str):

        extension = Path(file_name).suffix.lower()

        processor = self.processors.get(extension)

        if processor is None:

            logger.warning(

                f"No processor available for '{extension}'."

            )

            return None

        logger.info(

            f"Using {processor.__class__.__name__} for {file_name}"

        )

        return processor

    # =====================================================
    # Supported Extensions
    # =====================================================

    def supported_extensions(self):

        return sorted(self.processors.keys())