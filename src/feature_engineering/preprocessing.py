import pandas as pd
from loguru import logger

def align_and_fill_data(df: pd.DataFrame, fill_method: str = "ffill") -> pd.DataFrame:
    if fill_method == "ffill":
        df_filled = df.ffill()
    elif fill_method == "bfill":
        df_filled = df.bfill()
    else:
        df_filled = df.interpolate(method='time')

    df_filled = df_filled.fillna(0)
    
    logger.info(f"[preprocessing] Handled missing data using: {fill_method}")
    return df_filled
