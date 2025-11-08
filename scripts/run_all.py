import sys
import pandas as pd
from pathlib import Path

from loguru import logger

file_path = Path(__file__).resolve()
project_root = file_path.parent.parent
sys.path.append(str(project_root))

from src.data_ingestion.pipeline import run_data_ingestion_pipeline
from src.feature_engineering.pipeline import build_features_table
from run_training import training

def main():
    logger.info(f"[scripts/data_ingestion] Starting data ingestion pipeline.")
    run_data_ingestion_pipeline()
    logger.info(f"[scripts/data_ingestion] Finished data ingestion pipeline.")

    logger.info(f"[scripts/feature_engineering] Starting building features table pipeline.")
    final_features: pd.DataFrame = build_features_table()
    logger.info(f"[scripts/feature_engineering] Finished building features table pipeline.")
    training()


if __name__ == "__main__":
    main()
