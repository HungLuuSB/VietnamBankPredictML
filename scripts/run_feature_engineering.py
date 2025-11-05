import sys
import pandas as pd
from pathlib import Path

from loguru import logger

file_path = Path(__file__).resolve()
project_root = file_path.parent.parent
sys.path.append(str(project_root))

from src.feature_engineering.pipeline import build_features_table

def main():
    logger.info(f"[scripts/data_ingestion] Starting data ingestion pipeline.")
    final_features: pd.DataFrame = build_features_table()
    logger.info(f"[scripts/data_ingestion] Finished data ingestion pipeline.")
    print(final_features[:-100])

if __name__ == "__main__":
    main()
