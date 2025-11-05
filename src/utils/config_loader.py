from pathlib import Path
import yaml
from pathlib import Path
from dotenv import load_dotenv
import os
from loguru import logger

class ConfigLoader:
    _config: dict | None = None

    def __init__(self, config_path: str = "config/project_config.yml") -> None:
        self.config_path = Path(config_path)
        
        env_path = Path(__file__).resolve().parent.parent.parent / '.env'
        load_dotenv(dotenv_path=env_path)
        logger.info(f"Loaded .env from: {env_path.resolve()}")

    def _load_config(self) -> dict:
        if not self.config_path.exists():
            logger.error(f"[config_loader]: Could not find config file at: {self.config_path}")
            raise FileNotFoundError(f"Could not find config file at: {self.config_path}")
            
        with open(self.config_path, 'r') as f:
            config_data = yaml.safe_load(f)
        logger.info(f"[config_loader] Loaded config from {self.config_path.resolve()}")
       
        db_uri_env = os.getenv("DATABASE_URI")
        if db_uri_env:
            config_data['database']['uri'] = db_uri_env
            logger.info("[config_loader] Overwritten [database][uri] from .env")
            
        target_stock_env = os.getenv("TARGET_STOCK")
        if target_stock_env:
            config_data['project']['target_stock'] = target_stock_env
            logger.info("[config_loader] Overwritten [project][target_stock] from .env")
        
        return config_data

    def get_config(self) -> dict:
        if self._config is None:
            self._config = self._load_config()
        return self._config

try:
    config_loader = ConfigLoader()
    config = config_loader.get_config()
except Exception as e:
    logger.critical(f"[config_loader] Could not load config: {e}")
    raise
