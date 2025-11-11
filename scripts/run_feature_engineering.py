import sys
import pandas as pd
from pathlib import Path

from loguru import logger

file_path = Path(__file__).resolve()
project_root = file_path.parent.parent
sys.path.append(str(project_root))

from src.utils.config_loader import config
from src.feature_engineering.pipeline import build_features_table

def main():
    symbol = config["project"]["target_stock"]
    horizon = config["project"]["prediction_horizon_days"]
    logger.info(f"[scripts/feature_engineering] Starting building features table pipeline.")
    final_features: pd.DataFrame = build_features_table(symbol, horizon)
    logger.info(f"[scripts/feature_engineering] Finished building features table pipeline.")
    print(final_features)

if __name__ == "__main__":
    main()
