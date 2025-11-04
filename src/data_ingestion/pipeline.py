from numpy import log
from .collectors import VnStockCollector, YFinanceCollector
from .storage import SQLStorage
from loguru import logger

VN_STOCKS = ['VCB', 'BID', 'CTG', 'TCB']
GLOBAL_ASSETS = {
    '^GSPC': 'SP500',      # S&P 500
    '^DJI': 'DowJones',    # Dow Jones
    'GC=F': 'Gold',        # Vàng
    'CL=F': 'CrudeOil'     # Dầu thô
}
START_DATE = "2020-01-01"
END_DATE = "2025-11-04"

def run_data_ingestion_pipeline():
    logger.info(f"[data_ingestion_pipeline] Starting data ingestion process.")
    
    vn_collector = VnStockCollector()
    yf_collector = YFinanceCollector()
    storage = SQLStorage()
    
    for ticker in VN_STOCKS:
        df = vn_collector.fetch_stock_data(ticker, START_DATE, END_DATE)
        storage.save_data(df, table_name=ticker, if_exists="replace")

    for ticker in GLOBAL_ASSETS:
        df = yf_collector.fetch_global_data(ticker, START_DATE, END_DATE)
        storage.save_data(df, table_name=ticker, if_exists="replace")

    logger.success(f"[data_ingestion_pipeline] Finished data ingestion pipeline.")
