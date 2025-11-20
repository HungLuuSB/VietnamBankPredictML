import yaml
import sys
from pathlib import Path
from typing import Any, Dict
from loguru import logger

class ConfigLoader:
    def __init__(self, config_filename: str = "settings.yaml"):
        self.project_root = Path(__file__).resolve().parents[2]
        self.config_path = self.project_root / "config" / config_filename
        logger.add("logs/system.log", rotation="10 MB") 

        self._config = self._load_config()

    def _load_config(self) -> Dict[str, Any]:
        if not self.config_path.exists():
            logger.critical(f"[ConfigLoader] Config file not found at: {self.config_path}")
            raise FileNotFoundError(f"Config file missing: {self.config_path}")

        try:
            with open(self.config_path, "r", encoding="utf-8") as f:
                config_data = yaml.safe_load(f)
                logger.debug(f"[ConfigLoader] Successfully loaded config from {self.config_path}")
                return config_data
        except yaml.YAMLError as e:
            logger.error(f"[ConfigLoader] YAML syntax error in {self.config_path}: {e}")
            raise
        except Exception as e:
            logger.error(f"[ConfigLoader] Unexpected error while reading config: {e}")
            raise RuntimeError(f"Failed to load config: {e}")

    def get_config(self) -> Dict[str, Any]:
        return self._config

    def get(self, key: str, default=None) -> Any:
        val = self._config.get(key, default)
        if val is None:
            logger.warning(f"[ConfigLoader] Key '{key}' not found in config, using default: {default}")
        return val

try:
    config_loader = ConfigLoader()
    config = config_loader.get_config()
except Exception as e:
    logger.warning(f"[ConfigLoader] Failed to initialize config instance immediately. Error: {e}")
    config = {}
