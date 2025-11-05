import pandas as pd
from ..data_ingestion.storage import SQLStorage
from loguru import logger
from .preprocessing import align_and_fill_data
from . import transformers as tf
TARGET_STOCK = "VCB"

GLOBAL_ASSETS = {
    '^GSPC': 'SP500',      # S&P 500
    '^DJI': 'DowJones',    # Dow Jones
    'GC=F': 'Gold',        # Vàng
    'CL=F': 'CrudeOil'     # Dầu thô
}

START_DATE = "2020-01-01"
END_DATE = "2025-11-04"

PREDICTION_HORIZON_DAYS = 5

def build_features_table() -> pd.DataFrame:
    logger.info(f"[feature_engineering] Starting feature engineering pipeline...")
    storage = SQLStorage()
    
    df_target = storage.load_data(TARGET_STOCK, start_date=START_DATE, end_date=END_DATE)
    if df_target is None or df_target.empty:
        logger.error(f"[feature_engineering] Error while retrieving data for {TARGET_STOCK}.")
        return pd.DataFrame()

    df = pd.DataFrame(index=df_target.index)
    df[f"{TARGET_STOCK}_close"] = df_target['close']
    
    for asset in GLOBAL_ASSETS:
        df_asset = storage.load_data(asset, start_date=START_DATE, end_date=END_DATE)
        if not df_asset.empty:
            asset_series = df_asset['close'].rename(f"{asset}_close") #type: ignore
            df = df.join(asset_series, how='left')
        else:
            logger.warning(f"Empty data for features of: {asset}")
    
    df_filled = align_and_fill_data(df)

    base_price = pd.Series(df_filled[f"{TARGET_STOCK}_close"])
    
    df_filled['sma_20'] = tf.simple_moving_average(base_price, window=20)
    df_filled['ema_50'] = tf.exponential_moving_average(base_price, window=50)
    df_filled['returns_1d'] = tf.percentage_change(base_price, periods=1)
    df_filled['lag_1d'] = tf.lag_features(base_price, lag=1)
    
    if 'GSPC_close' in df_filled:
        df_filled['gspc_lag_1d'] = tf.lag_features(pd.Series(df_filled['GSPC_close']), lag=1)

    df_filled['target_price'] = tf.target_variable(
        base_price, 
        horizon=PREDICTION_HORIZON_DAYS
    )
    df_final = df_filled.dropna()
    logger.success(f"[feature_engineering] Successfully created features with {len(df_final)} line(s).")
    
    processed_table_name = f"processed_{TARGET_STOCK.lower()}_features"
    storage.save_data(df_final, processed_table_name, if_exists="replace")
    
    return df_final
    
