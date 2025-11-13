from datetime import date
import pandas as pd
import yfinance as yf
from vnstock import Vnstock
from loguru import logger

START_DATE = "2010-01-01"
END_DATE = date.today().isoformat()

class VnStockCollector:
    def fetch_stock_data(self, symbol: str) -> pd.DataFrame:
        try:
            stock = Vnstock().stock(symbol)
            df: pd.DataFrame = stock.quote.history(start=START_DATE, end=END_DATE)
            if df is None or df.empty:
                return pd.DataFrame()
            df = df.rename(columns={'time': 'date'})
            return df
        except Exception as e:
            return pd.DataFrame()

class YFinanceCollector:
    def fetch_stock_data(self, symbol: str) -> pd.DataFrame:
        try:
            ticker = yf.Ticker(symbol)
            df = ticker.history(start=START_DATE, end=END_DATE)
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
            return df
        except Exception as e:
            return pd.DataFrame()
