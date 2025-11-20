from src.utils.config_loader import config
from vnstock import Vnstock
from datetime import date
from loguru import logger
from .base_collector_model import BaseCollectorModel
import pandas as pd

class VnStockCollector(BaseCollectorModel):
    def fetch_data(self, symbol: str, start_date: str= "2010-01-01", end_date: str = date.today().isoformat()):
        logger.info(f"[VnStock] Fetching historical data for {symbol} from {start_date} to {end_date}...")
        try:
            stock = Vnstock().stock(symbol=symbol.upper())
            df = stock.quote.history(
                start=start_date,
                end=end_date,
                resolution='1D'
            )
            if df is None or df.empty:
                logger.warning(f"[VnStock] No data found for {symbol}.")
                return pd.DataFrame()

            df.rename(columns={'time': 'date'}, inplace=True)
            df['date'] = pd.to_datetime(df['date']).dt.strftime('%Y-%m-%d')
            df['stock_code'] = symbol.upper()
            required_cols = ['date', 'open', 'high', 'low', 'close', 'volume', 'symbol']
            final_cols = [c for c in required_cols if c in df.columns]
            logger.success(f"[VnStock] Successfully fetched {len(df)} rows for {symbol}.")
            return df[final_cols]

        except Exception as e:
            logger.error(f"[VnStock] Failed to fetch data for {symbol}. Error: {e}")
            return pd.DataFrame()
