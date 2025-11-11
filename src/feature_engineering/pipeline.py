import pandas as pd
from loguru import logger
from src.data_ingestion.storage import SQLStorage
from . import transformers as tf
from . import features_engineering_helpers

from src.utils.config_loader import config

def build_features_table(symbol: str, horizon=1) -> pd.DataFrame:
    df = features_engineering_helpers.process_features(symbol=symbol)
    df_final = features_engineering_helpers.process_target_variable(symbol, df, horizon)
    if df_final.empty:
        logger.error(f"[feature_engineering] Errors in pipeline.")
        return pd.DataFrame()
    logger.success(f"[feature_engineering] successfully created features with {len(df)} line(s).")
    processed_table_name = f"processed_{symbol.lower()}_features"
    storage = SQLStorage(db_uri=config["database"]["uri"])
    storage.save_data(df_final, processed_table_name, if_exists="replace")
    
    return df_final
