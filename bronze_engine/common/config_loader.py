"""
Loads YAML dataset configurations.
"""

from pathlib import Path
import yaml

from bronze_engine.common.logger import get_logger

logger = get_logger(__name__)


class ConfigLoader:
    def __init__(self, config_directory: str):
        self.config_directory = Path(config_directory)

    def load(self, config_name: str) -> dict:
        """
        Load a YAML configuration file.

        Example:
            loader.load("figure_nz")
        """

        config_path = self.config_directory / f"{config_name}.yaml"

        if not config_path.exists():
            raise FileNotFoundError(
                f"Configuration not found: {config_path}"
            )

        with open(config_path, "r", encoding="utf-8") as file:
            config = yaml.safe_load(file)

        logger.info(f"Loaded configuration: {config_path.name}")

        return config


if __name__ == "__main__":
    loader = ConfigLoader("bronze_engine/config/datasets")

    try:
        config = loader.load("figure_nz")
        print(config)
    except Exception as e:
        logger.error(e)