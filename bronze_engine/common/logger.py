"""
Central logging configuration for the NZ Sports Data Platform.
Every connector, loader, DAG and transformation will use this logger.
"""

import logging
import sys
from pathlib import Path


LOG_DIR = Path("logs")
LOG_DIR.mkdir(exist_ok=True)

LOG_FILE = LOG_DIR / "nz_sports_platform.log"


def get_logger(name: str) -> logging.Logger:
    """
    Returns a configured logger.

    Example:
        logger = get_logger(__name__)
        logger.info("Starting ingestion...")
    """

    logger = logging.getLogger(name)

    if logger.handlers:
        return logger

    logger.setLevel(logging.INFO)

    formatter = logging.Formatter(
        fmt="%(asctime)s | %(levelname)s | %(name)s | %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )

    # Console Output
    console_handler = logging.StreamHandler(sys.stdout)
    console_handler.setFormatter(formatter)

    # File Output
    file_handler = logging.FileHandler(LOG_FILE, encoding="utf-8")
    file_handler.setFormatter(formatter)

    logger.addHandler(console_handler)
    logger.addHandler(file_handler)

    logger.propagate = False

    return logger


# Test Logger
if __name__ == "__main__":
    logger = get_logger("Logger Test")
    logger.info("Logger initialized successfully.")
    logger.warning("Warning message.")
    logger.error("Error message.")