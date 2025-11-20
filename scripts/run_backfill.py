import sys
from pathlib import Path
from datetime import datetime,date


BASE_DIR = Path(__file__).resolve().parents[1]
sys.path.append(str(BASE_DIR))

from pyspark.sql.functions import col, regexp_replace
from pyspark.sql.types import StringType
from loguru import logger

from src.utils.spark_utils import get_spark_session, STOCK_SCHEMA
from src.utils.config_loader import config
from src.collectors import VnStockCollector
from src.collectors import YFinanceCollector
from src.collectors import VietFinCollector
from src.collectors import BaseCollectorModel

def save_to_lake(spark, symbol: str, collector: BaseCollectorModel, start_date: str, end_date: str, raw_path: str):
    logger.info(f"[Backfill] Processing symbol: {symbol}")
    pdf = collector.fetch_data(symbol, start_date, end_date)
    if pdf.empty:
        logger.warning(f"[Backfill] No data found for {symbol}. Skipping.")
        return
    try:
        numeric_cols = ['open', 'high', 'low', 'close']
        for col_name in numeric_cols:
            if col_name in pdf.columns:
                pdf[col_name] = pdf[col_name].astype(dtype=float)
        pdf['date'] = pdf['date'].astype(str)
        pdf.info()
        df = spark.createDataFrame(pdf, schema=STOCK_SCHEMA)
        df_final = df \
            .withColumn("symbol", regexp_replace(col("symbol"), "[^a-zA-Z0-9]", "_")) \
            .withColumn("date", col("date").cast(StringType()))
        df_final.write \
            .mode("append") \
            .partitionBy("symbol") \
            .parquet(raw_path)
        df_final.show(5)
        logger.success(f"[Backfill] Successfully saved {symbol} to Data Lake.")
    except Exception as e:
        logger.error(f"[Backfill] Error processing {symbol}: {e}")

def run():
    logger.info("[Backfill] Starting Historical Data Ingestion...")
    spark = get_spark_session("Backfill_History")
    try:
        storage_conf = config['storage']
        project_conf = config['project']

        BASE_PATH = storage_conf['hdfs_base']
        RAW_PATH = f"{BASE_PATH}/{storage_conf['raw_zone']}"
        TARGET_STOCKS = project_conf['target_stocks']
        GLOBAL_ASSETS = project_conf['global_assets']
        START_DATE = "2010-01-01"
        END_DATE = date.today().isoformat()

        logger.info(f"[Backfill] Target Path: {RAW_PATH}")
        logger.info(f"[Backfill] Time Range: {START_DATE} to {END_DATE}")
    except KeyError as e:
        logger.critical(f"[Backfill] Configuration error: {e}")
        sys.exit(1)

    #vn_collector = VnStockCollector()
    vn_collector = VietFinCollector()
    yf_collector = YFinanceCollector()

    logger.info(f"[Backfill] Fetching {len(TARGET_STOCKS)} Vietnam stocks...")
    for stock in TARGET_STOCKS:
        save_to_lake(spark, stock, vn_collector, START_DATE, END_DATE, RAW_PATH)

    logger.info(f"[Backfill] Fetching {len(GLOBAL_ASSETS)} Global assets...")
    for asset in GLOBAL_ASSETS:
        save_to_lake(spark, asset, yf_collector, START_DATE, END_DATE, RAW_PATH)

    logger.success("[Backfill] Historical Data Ingestion Completed.")
    spark.stop()

if __name__ == "__main__":
    run()
