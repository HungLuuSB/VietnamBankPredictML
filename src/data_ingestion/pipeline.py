from numpy import log
from .collectors import VnStockCollector, YFinanceCollector
from .storage import SQLStorage
from loguru import logger
from src.utils.config_loader import config

VN_STOCKS = config["data_sources"]["vn_stocks"]
GLOBAL_ASSETS = config["data_sources"]["global_assets"]
START_DATE = config["project"]["start_date"]
END_DATE = config["project"]["end_date"]

def run_data_ingestion_pipeline():
    logger.info(f"[data_ingestion_pipeline] Starting data ingestion process.")
    
    vn_collector = VnStockCollector()
    yf_collector = YFinanceCollector()
    storage = SQLStorage(db_uri=config["database"]["uri"])
    
    for ticker in VN_STOCKS:
        df = vn_collector.fetch_stock_data(ticker, START_DATE, END_DATE)
        storage.save_data(df, table_name=ticker, if_exists="replace")

    for ticker in GLOBAL_ASSETS:
        df = yf_collector.fetch_global_data(ticker, START_DATE, END_DATE)
        storage.save_data(df, table_name=ticker, if_exists="replace")

    logger.success(f"[data_ingestion_pipeline] Finished data ingestion pipeline.")
