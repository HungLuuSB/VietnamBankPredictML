import pandas as pd
from ..data_ingestion.storage import SQLStorage
from loguru import logger
from .preprocessing import align_and_fill_data
from . import transformers as tf

from src.utils.config_loader import config

def build_features_table() -> pd.DataFrame:
    target_stock = config["project"]["target_stock"]
    global_assets = config["data_sources"]["global_assets"]
    start_date = config["project"]["start_date"]
    end_date = config["project"]["end_date"]

    horizon = config["project"]["prediction_horizon_days"]
    logger.info(f"[feature_engineering] Starting feature engineering pipeline...")
    storage = SQLStorage(db_uri=config["database"]["uri"])
    
    df_target = storage.load_data(target_stock, start_date=start_date, end_date=end_date)
    if df_target is None or df_target.empty:
        logger.error(f"[feature_engineering] Error while retrieving data for {target_stock}.")
        return pd.DataFrame()

    df = pd.DataFrame(index=df_target.index)
    print(df_target)
    df[f"{target_stock}_close"] = df_target['close']
    df["volume"] = df_target["volume"]
    vn_30_df = storage.load_data("VN30", start_date=start_date, end_date=end_date)
    vn_30_series = vn_30_df["close"].rename(f"VN30_close") #type: ignore
    df = df.join(vn_30_series, how="left")
    for asset in global_assets:
        df_asset = storage.load_data(asset, start_date=start_date, end_date=end_date)
        if not df_asset.empty:
            asset_series = df_asset['close'].rename(f"{asset}_close") #type: ignore
            df = df.join(asset_series, how='left')
        else:
            logger.warning(f"Empty data for features of: {asset}")
    
    df_filled = align_and_fill_data(df)

    base_price = pd.Series(df_filled[f"{target_stock}_close"])
    df_filled['sma_50'] = tf.simple_moving_average(base_price, window=50)
    df_filled['ema_50'] = tf.exponential_moving_average(base_price, window=50)
    df_filled['returns_1d'] = tf.percentage_change(base_price, periods=1)
    df_filled['lag_1d'] = tf.lag_features(base_price, lag=1)
    
    #if '^GSPC_close' in df_filled:
    #    df_filled['^gspc_lag_1d'] = tf.lag_features(pd.Series(df_filled['^GSPC_close']), lag=1)

    for global_asset in global_assets:
        if f"{global_asset.lower()}_close" in df_filled:
            df_filled[f'{global_asset.lower()}_lag_1d'] = tf.lag_features(pd.Series(df_filled[f'{global_asset.lower()}_close']), lag=1)

    df_filled['target_price'] = tf.target_variable(
        base_price, 
        horizon=horizon
    )
    #df_filled.drop(columns=[f'{target_stock}_close', 'returns_1d', 'lag_1d', 'sma_20', 'ema_50'], axis=1, inplace=True)
    print("dropped")
    df_final = df_filled.dropna()
    logger.success(f"[feature_engineering] Successfully created features with {len(df_final)} line(s).")
    processed_table_name = f"processed_{target_stock.lower()}_features"
    storage.save_data(df_final, processed_table_name, if_exists="replace")
    
    return df_final
