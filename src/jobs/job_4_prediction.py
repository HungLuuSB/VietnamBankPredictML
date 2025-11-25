#spark-submit     --packages org.postgresql:postgresql:42.6.0     src/jobs/job_4_prediction.py
import sys
import re
from pathlib import Path
import pandas as pd
from datetime import datetime, timedelta

BASE_DIR = Path(__file__).resolve().parents[2]
sys.path.append(str(BASE_DIR))

from pyspark.sql import SparkSession
from pyspark.sql.functions import lag, col, lit, when, last, max as spark_max
from pyspark.sql.types import StructType, StructField, DoubleType, StringType
from pyspark.sql.window import Window
from pyspark.ml import PipelineModel
from loguru import logger

from src.utils.config_loader import config
from src.utils.spark_utils import get_spark_session

def calculate_technical_indicators(pdf: pd.DataFrame) -> pd.DataFrame:
    pdf = pdf.sort_values(by="date")
    pdf["sma_20"] = pdf["close"].rolling(window=20).mean()
    pdf["ema_50"] = pdf["close"].ewm(span=50, adjust=False).mean()
    return pdf

def run():
    logger.info("[Job 4] Starting Batch Prediction (Full Fix)...")

    # 1. Load Config
    try:
        storage_conf = config['storage']
        db_conf = config['database']
        project_conf = config['project']
        BASE_PATH = storage_conf['hdfs_base']
        RAW_PATH = f"{BASE_PATH}/{storage_conf['raw_zone']}"
        MODEL_BASE_PATH = f"{BASE_PATH}/{storage_conf['model_zone']}"
        RAW_GLOBAL_ASSETS = project_conf['global_assets']
        CLEAN_GLOBAL_ASSETS = [re.sub(r"[^a-zA-Z0-9]", "_", s) for s in RAW_GLOBAL_ASSETS]
        TARGET_STOCKS = project_conf['target_stocks']
        JDBC_URL = db_conf['postgres_url']
        DB_PROPS = {
            "user": db_conf['postgres_user'],
            "password": db_conf['postgres_pass'],
            "driver": db_conf['jdbc_driver_path']
        }
        logger.info(f"Global Assets: {CLEAN_GLOBAL_ASSETS}")
    except KeyError as e:
        logger.critical(f"Configuration error: {e}")
        sys.exit(1)

    spark = get_spark_session("Prediction_Batch")
    try:
        df_raw = spark.read.option("mergeSchema", "true").parquet(RAW_PATH)
        df_raw = df_raw.withColumn("date", col("date").cast(StringType()))
        df_raw = df_raw.dropDuplicates(["symbol", "date"])
    except Exception as e:
        logger.critical(f"Failed to read Data Lake: {e}")
        sys.exit(1)

    w_global = Window.orderBy("date")
    global_dfs = {}
    for asset in CLEAN_GLOBAL_ASSETS:
        df_asset = df_raw.filter(col("symbol") == asset) \
            .withColumn(f"{asset}_lag_1d", lag("close", 1).over(w_global)) \
            .select("date", f"{asset}_lag_1d")
        global_dfs[asset] = df_asset

    logger.info("🔍 DEBUGGING GLOBAL ASSETS DATA:")
    for asset, df in global_dfs.items():
        count = df.count()
        logger.info(f"   Asset: {asset} | Rows: {count}")
        if count > 0:
            # In ra 5 ngày mới nhất của mã này xem có dữ liệu không
            df.orderBy(col("date").desc()).show(5)
        else:
            logger.warning(f"   ⚠️ {asset} IS EMPTY! Check Backfill or Symbol Name.")
    df_targets = df_raw.filter(col("symbol").isin(TARGET_STOCKS))
    w_target = Window.partitionBy("symbol").orderBy("date")
    df_features = df_targets \
        .withColumn("lag_1d", lag("close", 1).over(w_target)) \
        .withColumn("returns_1d", when(col("lag_1d").isNotNull(), (col("close") - col("lag_1d"))/col("lag_1d")).otherwise(0)) \

    output_schema = StructType(df_features.schema.fields + [
        StructField("sma_20", DoubleType()), StructField("ema_50", DoubleType())
    ])
    df_indicators = df_features.groupBy("symbol").applyInPandas(calculate_technical_indicators, schema=output_schema)

    df_final = df_indicators
    for asset_name, df_asset in global_dfs.items():
        df_final = df_final.join(df_asset, on="date", how="left")

    w_ffill = Window.partitionBy("symbol").orderBy("date") \
        .rowsBetween(Window.unboundedPreceding, Window.currentRow)

    cols_to_ffill = [f"{a}_lag_1d" for a in CLEAN_GLOBAL_ASSETS]
    
    for col_name in cols_to_ffill:
        df_final = df_final.withColumn(col_name, last(col_name, ignorenulls=True).over(w_ffill))
        df_final = df_final.fillna(0.0, subset=[col_name])

    df_latest_features = df_final \
        .withColumn("max_date", spark_max("date").over(Window.partitionBy("symbol"))) \
        .filter(col("date") == col("max_date")) \
        .drop("max_date")
    
    #df_latest_features = df_latest_features.na.drop(subset=["ema_50"])
    count = df_latest_features.count()
    if count > 0:
        logger.info(f"-> Ready to predict for {count} stocks.")
        show_cols = ["date", "symbol", "volume", "close", "lag_1d", "returns_1d", "sma_20", "ema_50"] + cols_to_ffill
        df_latest_features.select(show_cols).show(truncate=False)
    else:
        logger.warning("No valid latest data found (Maybe missing EMA_50 due to insufficient history?). Exiting.")
        return

    logger.info("-> Calculating Top Rankings...")
    df_rankings = df_latest_features.select("symbol", "returns_1d", "date") \
        .orderBy(col("returns_1d").desc()).limit(10)
        
    df_rank_export = df_rankings \
        .withColumn("metric_type", lit("tssl_1d")) \
        .withColumnRenamed("returns_1d", "metric_value") \
        .select("symbol", "metric_value", "metric_type", "date")

    try:
        df_rank_export.write.jdbc(url=JDBC_URL, table="stock_rankings", mode="overwrite", properties=DB_PROPS)
        logger.success("Synced 'stock_rankings'.")
    except Exception as e:
        logger.error(f"Failed to save rankings: {e}")

    logger.info("-> Running Predictions...")
    predictions_list = []
    rows = df_latest_features.collect()
    for row in rows:
        stock = row['symbol']
        model_path = f"{MODEL_BASE_PATH}/{stock}_lr_model"
        
        try:
            model = PipelineModel.load(model_path)
            df_single = spark.createDataFrame([row], schema=df_latest_features.schema)
            prediction_df = model.transform(df_single)
            pred_row = prediction_df.select("prediction").first()
            pred_price = pred_row['prediction'] #type: ignore
            pred_date = row['date']
            logger.info(f"   [{stock}] Date: {pred_date} | Close: {row['close']} -> Pred: {pred_price:,.2f}")
            predictions_list.append({
                "stock_code": stock,
                "predicted_price": float(pred_price),
                "prediction_date": pred_date
            })
        except Exception as e:
            logger.warning(f"   [{stock}] Predict error: {e}")

    # 3. Save Predictions
    if predictions_list:
        schema_pred = StructType([
            StructField("stock_code", StringType(), True),
            StructField("predicted_price", DoubleType(), True),
            StructField("prediction_date", StringType(), True)
        ])
        df_preds = spark.createDataFrame(predictions_list, schema=schema_pred)
        try:
            df_preds.write.jdbc(url=JDBC_URL, table="stock_predictions", mode="append", properties=DB_PROPS)
            logger.success(f"Saved {len(predictions_list)} predictions to DB.")
        except Exception as e:
            logger.error(f"Failed to save predictions DB: {e}")
    spark.stop()

if __name__ == "__main__":
    run()
