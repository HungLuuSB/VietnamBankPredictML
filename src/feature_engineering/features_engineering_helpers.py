import pandas as pd
import numpy as np
from .preprocessing import align_and_fill_data
from . import transformers as tf
from src.data_ingestion.storage import SQLStorage
from src.utils.config_loader import config
from loguru import logger
from src.data_ingestion import data_helpers
try:
    _db_uri = config['database']['uri']
    _storage = SQLStorage(db_uri=_db_uri)
except Exception as e:
    _storage = None

def process_features(symbol: str) -> pd.DataFrame:
    if _storage is None:
        logger.error(f"Storage has not been initialized yet.")
        return pd.DataFrame()
    target_stock = symbol
    global_assets = config["data_sources"]["global_assets"]
    
    df_target = _storage.load_data(table_name=target_stock)
    if df_target.empty:
        return pd.DataFrame()

    df = pd.DataFrame(index=df_target.index)
    df[f"{target_stock}_close"] = df_target['close']
    df[f"volume"] = df_target['volume']

    vn_30_df = _storage.load_data("VN30")
    vn_30_series = vn_30_df["close"].rename(f"VN30_close") #type: ignore
    df = df.join(vn_30_series, how="left")
    for asset in global_assets:
        df_asset = data_helpers.get_stock_data(asset)
        if not df_asset.empty:
            asset_series = df_asset['close'].rename(f"{asset}_close") #type: ignore
            df = df.join(asset_series, how='left')
        else:
            logger.warning(f"No data for: {asset}")
    df_filled = align_and_fill_data(df, fill_method="ffill")
    base_price = pd.Series(df_filled[f"{target_stock}_close"])
    df_filled['sma_50'] = tf.simple_moving_average(base_price, window=50)
    df_filled['ema_50'] = tf.exponential_moving_average(base_price, window=50)
    df_filled['returns_1d'] = tf.percentage_change(base_price, periods=1)
    df_filled['lag_1d'] = tf.lag_features(base_price, lag=1)
    for global_asset in global_assets:
        if f"{global_asset}_close" in df_filled:
            df_filled[f'{global_asset}_lag_1d'] = tf.lag_features(pd.Series(df_filled[f'{global_asset}_close']), lag=1)

    for global_asset in global_assets:
        if f"{global_asset}_close" in df_filled:
            df_filled.drop(columns=[f"{global_asset}_close"], inplace=True)
    df_final = df_filled.dropna()
    return df_final

def process_target_variable(symbol: str, df: pd.DataFrame, horizon: int = 1) -> pd.DataFrame:
    base_price = pd.Series(df[f"{symbol}_close"])
    df['target_price'] = tf.target_variable(
        base_price, 
        horizon=horizon
    )
    df.dropna(inplace=True)
    return df


def process_today_features(symbol: str) -> np.ndarray | None:
    if _storage is None:
        logger.error(f"Storage has not been initialized yet.")
        return None
    
    data = process_features(symbol)
    if data.empty:
        return None
    
    today_features_vector = data.iloc[-1].values
    return today_features_vector

