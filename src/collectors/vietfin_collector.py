from src.utils.config_loader import config
from vietfin import vf
from datetime import date
from loguru import logger
from .base_collector_model import BaseCollectorModel
import pandas as pd

class VietFinCollector(BaseCollectorModel):
    def fetch_data(self, symbol: str, start_date: str = "2010-01-01", end_date: str = date.today().isoformat()) -> pd.DataFrame:
        symbol = symbol.upper()
        try:
            logger.info(f"[VietFin] Fetching historical data for {symbol} from {start_date} to {end_date}...")
            df = vf.equity.price.historical(symbol=symbol, start_date=start_date, end_date=end_date).to_df()
            if df is None or df.empty:
                logger.warning(f"[VietFin] No data found for {symbol}.")
                return pd.DataFrame()
            df["date"] = df.index
            df["symbol"] = symbol
            df.reset_index(drop=True, inplace=True)
            cols = ["date", "open", "high", "low", "close", "volume", "symbol"]
            df_final = pd.DataFrame()
            for col in cols:
                df_final[col] = df[col]
            return df_final
        except Exception as e:
            logger.error(f"[VietFin] Failed to fetch data for {symbol}. Error: {e}")
            return pd.DataFrame()
