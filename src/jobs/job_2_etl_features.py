import sys
import re
from pathlib import Path
import pandas as pd

BASE_DIR = Path(__file__).resolve().parents[2]
sys.path.append(str(BASE_DIR))

from pyspark.sql import SparkSession
from pyspark.sql.functions import lag, col, lit, when, last, avg
from pyspark.sql.pandas.functions import pandas_udf, PandasUDFType
from pyspark.sql.types import StructType, StructField, DoubleType, StringType
from pyspark.sql.window import Window
from loguru import logger

from src.utils.config_loader import config
from src.utils.spark_utils import get_spark_session

def calculate_technical_indicators(pdf: pd.DataFrame) -> pd.DataFrame:
    pdf = pdf.sort_values(by="date")
    pdf["sma_20"] = pdf["close"].rolling(window=20).mean()
    pdf["ema_50"] = pdf["close"].ewm(span=50, adjust=False).mean()
    return pdf

def run():
    logger.info("[Job 2] Starting ETL & Feature Engineering Pipeline...")
    try:
        storage_conf = config['storage']
        db_conf = config['database']
        project_conf = config['project']
        BASE_PATH = storage_conf['hdfs_base']
        RAW_PATH = f"{BASE_PATH}/{storage_conf['raw_zone']}"
        PROCESSED_PATH = f"{BASE_PATH}/{storage_conf['processed_zone']}"
        RAW_GLOBAL_ASSETS = project_conf['global_assets']
        CLEAN_GLOBAL_ASSETS = [re.sub(r"[^a-zA-Z0-9]", "_", s) for s in RAW_GLOBAL_ASSETS]
        logger.info(f"Config loaded. Raw Path: {RAW_PATH}")
    except KeyError as e:
        logger.critical(f"Configuration error: {e}")
        sys.exit(1)

    spark = get_spark_session("FeatureEngineering_Batch")

    try:
        df_raw = spark.read \
            .option("mergeSchema", "true") \
            .option("basePath", RAW_PATH) \
            .parquet(f"{RAW_PATH}/*")
        df_raw = df_raw.dropDuplicates(["symbol", "date"])
        df_raw = df_raw.withColumn("date", col("date").cast(StringType()))
        logger.info(f"Loaded {df_raw.count()} raw records from Data Lake.")
    except Exception as e:
        logger.critical(f"Failed to read Data Lake: {e}")
        sys.exit(1)
    df_raw.show(20)
    fill_window = Window.orderBy("date").rowsBetween(Window.unboundedPreceding, Window.currentRow)
    global_dfs = {}

    for asset in CLEAN_GLOBAL_ASSETS:
        w = Window.orderBy("date")
        df_asset = df_raw.filter(col("symbol") == asset) \
            .withColumn(f"{asset}_lag_raw", lag("close", 1).over(w)) \
            .withColumn(f"{asset}_lag_1d", last(f"{asset}_lag_raw", ignorenulls=True).over(fill_window)) \
            .select("date", f"{asset}_lag_1d")

        global_dfs[asset] = df_asset

    df_targets = df_raw.filter(~col("symbol").isin(CLEAN_GLOBAL_ASSETS))
    w_target = Window.partitionBy("symbol").orderBy("date")
    df_features = df_targets \
        .withColumn("lag_1d", lag("close", 1).over(w_target)) \
        .withColumn("returns_1d", 
                    when(col("lag_1d").isNotNull(), (col("close") - col("lag_1d"))/col("lag_1d"))
                    .otherwise(0))

    output_schema = StructType(df_features.schema.fields + [
        StructField("sma_20", DoubleType()),
        StructField("ema_50", DoubleType())
    ])

    df_indicators = df_features.groupBy("symbol").applyInPandas(calculate_technical_indicators, schema=output_schema)

    df_final = df_indicators
    for asset_name, df_asset in global_dfs.items():
        df_final = df_final.join(df_asset, on="date", how="left")

    df_final = df_final.withColumn("target_price", lag("close", -1).over(w_target))

    df_final.orderBy(col("date").desc()) \
            .show(20, truncate=False)
    logger.info("-> Writing FULL history to PostgreSQL (Including Today)...")
    df_for_postgres = df_final.na.drop(subset=["close", "date"])
    unique_symbols = [row.symbol for row in df_for_postgres.select("symbol").distinct().collect()]
    jdbc_url = db_conf['postgres_url']
    db_props = {
        "user": db_conf['postgres_user'],
        "password": db_conf['postgres_pass'],
        "driver": db_conf['jdbc_driver_path']
    }
    cols_for_api = ["date", "open", "high", "low", "close", "volume"]

    for symbol in unique_symbols:
        table_name = re.sub(r"[^a-zA-Z0-9]", "", symbol)
        try:
            df_export = df_for_postgres.filter(col("symbol") == symbol).select(cols_for_api)
            df_export.write.jdbc(
                url=jdbc_url,
                table=table_name,
                mode="overwrite",
                properties=db_props
            )
            logger.info(f"   + Synced table '{table_name}' ({df_export.count()} rows)")
        except Exception as e:
            logger.error(f"   ! Failed to sync table '{table_name}': {e}")


    required_cols_training = ["target_price", "ema_50"] + [f"{a}_lag_1d" for a in CLEAN_GLOBAL_ASSETS]
    df_for_training = df_final.na.drop(subset=required_cols_training)
    cols_for_training = [
        "date", "symbol", "close", "volume",
        "returns_1d", "lag_1d", "sma_20", "ema_50", "target_price"
    ] + [f"{a}_lag_1d" for a in CLEAN_GLOBAL_ASSETS]
    logger.info(f"-> Writing CLEAN features to Data Lake: {PROCESSED_PATH}")
    df_for_training.select(cols_for_training).write \
        .mode("overwrite") \
        .partitionBy("symbol") \
        .parquet(PROCESSED_PATH)

    logger.success("[Job 2] Pipeline Completed Successfully.")

if __name__ == "__main__":
    run()
