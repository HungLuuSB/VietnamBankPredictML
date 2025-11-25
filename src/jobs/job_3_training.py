#spark-submit src/jobs/job_3_training.py
import sys
from pathlib import Path

BASE_DIR = Path(__file__).resolve().parents[2]
sys.path.append(str(BASE_DIR))

from pyspark.sql.functions import col, percent_rank
from pyspark.sql.window import Window
from pyspark.ml import Pipeline
from pyspark.ml.feature import VectorAssembler, StandardScaler
from pyspark.ml.regression import LinearRegression
from pyspark.ml.evaluation import RegressionEvaluator
from loguru import logger

from src.utils.config_loader import config
from src.utils.spark_utils import get_spark_session

def train_model_for_stock(spark, df_all, symbol: str, model_base_path: str, feature_cols: list):
    logger.info(f"[{symbol}] Starting training process...")
    df_stock = df_all.filter(col("symbol") == symbol)
    count = df_stock.count()
    if count < 50:
        logger.warning(f"[{symbol}] Not enough data to train (<50 rows). Skipping.")
        return

    w = Window.partitionBy("symbol").orderBy("date")
    df_ranked = df_stock.withColumn("rank", percent_rank().over(w))
    train_data = df_ranked.filter(col("rank") <= 0.8).drop("rank")
    test_data = df_ranked.filter(col("rank") > 0.8).drop("rank")
    logger.info(f"   + Train set: {train_data.count()} rows (Past)")
    logger.info(f"   + Test set:  {test_data.count()} rows (Future)")

    train_data, test_data = df_stock.randomSplit([0.8, 0.2], seed=42)

    assembler = VectorAssembler(
        inputCols=feature_cols, 
        outputCol="unscaled_features",
        handleInvalid="skip"
    )
    
    scaler = StandardScaler(
        inputCol="unscaled_features",
        outputCol="features",
        withStd=True,
        withMean=True
    )
    
    lr = LinearRegression(
        featuresCol="features", 
        labelCol="target_price",
        maxIter=100,
        regParam=0.0,
        elasticNetParam=0.0
    )

    pipeline = Pipeline(stages=[assembler, scaler, lr])

    try:
        model = pipeline.fit(train_data)
        predictions = model.transform(test_data)
        evaluator_rmse = RegressionEvaluator(labelCol="target_price", predictionCol="prediction", metricName="rmse")
        evaluator_r2 = RegressionEvaluator(labelCol="target_price", predictionCol="prediction", metricName="r2")
        rmse = evaluator_rmse.evaluate(predictions)
        r2 = evaluator_r2.evaluate(predictions)
        logger.info(f"[{symbol}] Training Success. RMSE: {rmse:,.2f} | R2: {r2:.4f}")
        save_path = f"{model_base_path}/{symbol}_lr_model"
        model.write().overwrite().save(save_path)
        logger.info(f"[{symbol}] Model saved to: {save_path}")

    except Exception as e:
        logger.error(f"❌ [{symbol}] Training Failed: {e}")

def run():
    logger.info("[Job 3] Starting Model Training Pipeline...")
    try:
        storage_conf = config['storage']
        project_conf = config['project']
        BASE_PATH = storage_conf['hdfs_base']
        PROCESSED_PATH = f"{BASE_PATH}/{storage_conf['processed_zone']}"
        MODEL_BASE_PATH = f"{BASE_PATH}/{storage_conf['model_zone']}"
        FEATURE_COLS = [
            'close',
            'volume',
            'returns_1d',
            'lag_1d',
            'sma_20',
            'ema_50',
            '_GSPC_lag_1d',
            'GC_F_lag_1d',
            '_DJI_lag_1d'
        ]
        
    except KeyError as e:
        logger.critical(f"Configuration error: {e}")
        sys.exit(1)

    spark = get_spark_session("ModelTraining_Batch")

    try:
        logger.info(f"-> Reading features from: {PROCESSED_PATH}")
        df_all = spark.read.parquet(PROCESSED_PATH)
        stock_rows = df_all.select("symbol").distinct().collect()
        all_symbols = [row.symbol for row in stock_rows]
        target_symbols = [s for s in all_symbols if not s.startswith("_") and "GC_" not in s]
        logger.info(f"-> Found {len(target_symbols)} stocks to train: {target_symbols}")
        logger.info(f"-> Input Features: {FEATURE_COLS}")

    except Exception as e:
        logger.critical(f"Failed to load data. Did you run Job 2? Error: {e}")
        sys.exit(1)

    for symbol in target_symbols:
        train_model_for_stock(spark, df_all, symbol, MODEL_BASE_PATH, FEATURE_COLS)

    logger.success("[Job 3] Model Training Pipeline Completed.")
    spark.stop()

if __name__ == "__main__":
    run()
