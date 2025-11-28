#spark-submit     --packages org.postgresql:postgresql:42.6.0     src/jobs/job_5_ranking.py
import sys
import re
from pathlib import Path
import pandas as pd
from datetime import datetime, timedelta

BASE_DIR = Path(__file__).resolve().parents[2]
sys.path.append(str(BASE_DIR))

from pyspark.sql.functions import col, lit, max as spark_max, desc
from loguru import logger

from src.utils.config_loader import config
from src.utils.spark_utils import get_spark_session

def run():
    logger.info("[Job 5] Starting Market Ranking Pipeline...")
    try:
        storage_conf = config['storage']
        db_conf = config['database']
        project_conf = config['project']
        BASE_PATH = storage_conf['hdfs_base']
        PROCESSED_PATH = f"{BASE_PATH}/{storage_conf['processed_zone']}"
        RAW_GLOBAL_ASSETS = project_conf['global_assets']
        CLEAN_GLOBAL_ASSETS = [re.sub(r"[^a-zA-Z0-9]", "_", s) for s in RAW_GLOBAL_ASSETS]
        JDBC_URL = db_conf['postgres_url']
        DB_PROPS = {
            "user": db_conf['postgres_user'],
            "password": db_conf['postgres_pass'],
            "driver": db_conf['jdbc_driver_path']
        }
    except KeyError as e:
        logger.critical(f"Configuration error: {e}")
        sys.exit(1)

    spark = get_spark_session("Ranking_Batch")

    try:
        logger.info(f"-> Reading features from: {PROCESSED_PATH}")
        df_all = spark.read.parquet(PROCESSED_PATH)
    except Exception as e:
        logger.critical(f"Failed to read Data Lake: {e}")
        sys.exit(1)

    try:
        latest_date_row = df_all.select(spark_max("date").alias("max_date")).first()
        if not latest_date_row or not latest_date_row["max_date"]:
             logger.warning("Data Lake appears to be empty or has no dates.")
             return
        latest_date = latest_date_row["max_date"]
        logger.info(f"-> Latest market date found: {latest_date}")
    except Exception as e:
        logger.error(f"Error finding max date: {e}")
        return

    df_ranking_source = df_all.filter(
        (col("date") == latest_date) & 
        (~col("symbol").isin(CLEAN_GLOBAL_ASSETS))
    )
    
    count = df_ranking_source.count()
    logger.info(f"-> Candidates for ranking: {count} stocks")

    if count == 0:
        logger.warning("No stocks found for ranking on this date.")
        return

    logger.info("-> Calculating Top 10 Gainers...")
    
    df_top_gainers = df_ranking_source.select("symbol", "returns_1d", "date") \
        .orderBy(col("returns_1d").desc()) \
        .limit(10)

    df_export = df_top_gainers \
        .withColumn("metric_type", lit("tssl_1d")) \
        .withColumnRenamed("returns_1d", "metric_value") \
        .select("symbol", "metric_value", "metric_type", "date")

    df_export.show(5, truncate=False)

    try:
        logger.info("-> Saving rankings to PostgreSQL...")
        df_export.write.jdbc(
            url=JDBC_URL,
            table="stock_rankings",
            mode="overwrite", 
            properties=DB_PROPS
        )
        logger.success("Synced 'stock_rankings' successfully.")
    except Exception as e:
        logger.error(f"Failed to save rankings to DB: {e}")
    spark.stop()

if __name__ == "__main__":
    run()
