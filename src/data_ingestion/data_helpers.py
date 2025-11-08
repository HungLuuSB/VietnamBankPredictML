import pandas as pd

from .storage import SQLStorage
from src.utils.config_loader import config
from loguru import logger
from datetime import date

try:
    _db_uri = config['database']['uri']
    _storage = SQLStorage(db_uri=_db_uri)
    logger.info(f"Data Helper: Đã khởi tạo kết nối CSDL tới {_db_uri}")
except Exception as e:
    logger.critical(f"Data Helper: Không thể khởi tạo CSDL! Lỗi: {e}")
    _storage = None

def today() -> str:
    return date.today().isoformat()

def get_stock_data(symbol: str, start_date: str | None = None, end_date: str | None = None) -> pd.DataFrame:
    if _storage is None:
        logger.error(f"Storage has not been initialized yet.")
        return pd.DataFrame()

    return _storage.load_data(table_name=symbol, start_date=start_date, end_date=end_date)

def get_processed_features(symbol: str) -> pd.DataFrame:
    if _storage is None:
        logger.error(f"Storage has not been initialized yet.")
        return pd.DataFrame()
    return _storage.load_data(table_name=f"processed_{symbol}_features", start_date=None, end_date=None)

