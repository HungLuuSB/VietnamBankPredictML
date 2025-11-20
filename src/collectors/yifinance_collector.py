import yfinance as yf
import pandas as pd
from datetime import date
from loguru import logger
from .base_collector_model import BaseCollectorModel

class YFinanceCollector(BaseCollectorModel):
    def fetch_data(self, symbol: str, start_date: str = "2010-01-01", end_date: str = date.today().isoformat()) -> pd.DataFrame:
        try:
            logger.info(f"[YFinance] Fetching historical data for {symbol} from {start_date} to {end_date}...")
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
            df['symbol'] = symbol
            return df
        except Exception as e:
            logger.error(f"[YFinance] Failed to fetch data for {symbol}. Error: {e}")
            return pd.DataFrame()
