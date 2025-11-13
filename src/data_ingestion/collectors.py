import pandas as pd
import yfinance as yf
from vnstock import Vnstock
from loguru import logger

from src.data_ingestion import data_helpers

class VnStockCollector:
    def fetch_stock_data(self, symbol: str, start_date: str, end_date: str) -> pd.DataFrame:
        logger.info(f"[vnstock] Retrieving data for {symbol} from {start_date} to {end_date}...")
        try:
            stock = Vnstock().stock(symbol)
            df: pd.DataFrame = stock.quote.history(start=start_date, end=end_date)
            if df is None or df.empty:
                logger.warning(f"[vnstock] Could not retrieve data for {symbol}.")
            logger.success(f"[vnstock] Successfully retrieved stock history for {symbol}")
            df = df.rename(columns={'time': 'date'})
            return df
        except Exception as e:
            logger.error(f"[vnstock] Error while retrieving data for {symbol}: {e}")
            return pd.DataFrame()

class YFinanceCollector:
    def fetch_global_data(self, symbol: str, start_date: str, end_date: str) -> pd.DataFrame:
        logger.info(f"[yfinance] Retrieving data from {symbol} from {start_date} to {end_date}...")
        try:
            ticker = yf.Ticker(symbol)
            df = ticker.history(start=start_date, end=end_date)
            df = df.reset_index()
            df = df.rename(columns={
                "Date": "date",
                "Open": "open",
                "High": "high",
                "Low": "low",
                "Close": "close",
                "Volume": "volume",
                "Dividends": "dividends",
                "Stock Splits": "stock splits",
            })
            df = df.drop(columns=["dividends", "stock splits"])
            df['date'] = pd.to_datetime(df['date']).dt.tz_localize(None)
            logger.success(f"[yfinance] Successfully retrieved stock history for {symbol}")
            return df
        except Exception as e:
            logger.error(f"[yfinance] Error while retrieving data for {symbol}: {e}")
            return pd.DataFrame()
