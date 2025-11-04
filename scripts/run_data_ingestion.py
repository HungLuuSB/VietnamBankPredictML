import sys
from pathlib import Path

from loguru import logger

file_path = Path(__file__).resolve()
project_root = file_path.parent.parent
sys.path.append(str(project_root))

from src.data_ingestion.pipeline import run_data_ingestion_pipeline

def main():
    logger.info(f"[scripts/data_ingestion] Starting data ingestion pipeline.")
    run_data_ingestion_pipeline()
    logger.info(f"[scripts/data_ingestion] Finished data ingestion pipeline.")
    pass

if __name__ == "__main__":
    main()
