from os.path import join
import sys
from pathlib import Path

from numpy import ones

BASE_DIR = Path(__file__).resolve().parent.parent
sys.path.append(str(BASE_DIR))

from loguru import logger
from utils.config_loader import config
from pyspark.sql import SparkSession
from pyspark.sql.types import StringType
from pyspark.sql.functions import lag, col, lit, when, last, avg, exp
from pyspark.sql.window import Window

def run_feature_engineering():
    BASE_PATH = config["infrastructure"]["hdfs_base_path"]
    RAW_PATH = f"{BASE_PATH}/raw_ticks/"
    PROCESSED_PATH = f"{BASE_PATH}/processed_features/"
    GLOBAL_ASSETS = config["project"]["global_assets"]
    POSTGRES_URL = config["infrastructure"]["postgres_url"]
    POSTGRES_USER = config["infrastructure"]["postgres_user"]
    POSTGRES_PASSWORD = config["infrastructure"]["postgres_pass"]

    spark = SparkSession.builder.appName("FeatureEngineering").getOrCreate() #type: ignore
    spark.sparkContext.setLogLevel("ERROR")
    try:
        df_raw = spark.read.option("mergeSchema", "true").parquet(RAW_PATH)
        df_raw = df_raw.withColumn("date", col("date").cast(StringType()))
        count = df_raw.count()
        if count == 0:
            return

    except Exception as e:
        logger.error(f"[feature_engineering] Error while reading from file: {e}")
        return

    fill_window = Window.orderBy("date").rowsBetween(Window.unboundedPreceding, Window.currentRow)
    gspc_window = Window.orderBy("date")
    df_gspc = df_raw.filter(col("symbol") == "_GSPC") \
        .withColumn("GSPC_lag_1d_raw", lag(col("close"), 1).over(gspc_window)) \
        .select("date", "GSPC_lag_1d_raw")
    
    df_gspc = df_gspc.withColumn(
        "GSPC_lag_1d", last("GSPC_lag_1d_raw", ignorenulls=True).over(fill_window)
    ).select("date", "GSPC_lag_1d")

    gold_window = Window.orderBy("date")
    df_gold = df_raw.filter(col("symbol") == "GC_F") \
        .withColumn("GC_F_lag_1d_raw", lag(col("close"), 1).over(gold_window)) \
        .select("date", "GC_F_lag_1d_raw")
        
    df_gold = df_gold.withColumn(
        "GC_F_lag_1d", last("GC_F_lag_1d_raw", ignorenulls=True).over(fill_window)
    ).select("date", "GC_F_lag_1d")


    dji_window = Window.orderBy("date")
    df_dji = df_raw.filter(col("symbol") == "_DJI") \
        .withColumn("_DJI_lag_1d_raw", lag(col("close"), 1).over(dji_window)) \
        .select("date", "_DJI_lag_1d_raw")
        
    df_dji = df_dji.withColumn(
        "_DJI_lag_1d", last("_DJI_lag_1d_raw", ignorenulls=True).over(fill_window)
    ).select("date", "_DJI_lag_1d")


    df_targets = df_raw.filter(~col("symbol").isin(["_GSPC", "GC_F", "_DJI"]))
    window_spec_targets = Window.partitionBy("symbol").orderBy("date")
    df_api_to_save = df_targets
    df_features = df_targets.withColumn("lag_1d", lag("close", 1).over(window_spec_targets))
    df_features = df_features.withColumn(
        "returns_1d", 
        when(col("lag_1d").isNotNull(), (col("close") - col("lag_1d")) / col("lag_1d"))
        .otherwise(lit(0))
    )
    df_features = df_features.drop("high", "low", "open")
    sma_20_window = Window.partitionBy("symbol").orderBy("date").rowsBetween(-19, 0)
    sma_50_window = Window.partitionBy("symbol").orderBy("date").rowsBetween(-49, 0)

    df_features = df_features.withColumn(
        "sma_20", avg("close").over(sma_20_window)
    )
    df_features = df_features.withColumn(
        "sma_50", avg("close").over(sma_50_window)
    )

    df_joined = df_features.join(df_gspc, on="date", how="left") \
        .join(df_gold, on="date", how="left") \
        .join(df_dji, on="date", how="left")

    df_final = df_joined.withColumn(
        "target_price", lag("close", -1).over(window_spec_targets)
    )
    
    df_final.show()
    df_final = df_final.na.drop(subset=[
        "target_price", "lag_1d", "GSPC_lag_1d", "GC_F_lag_1d", "_DJI_lag_1d"
    ])
    df_final.show()
    df_final.write \
        .mode("overwrite") \
        .partitionBy("symbol") \
        .parquet(PROCESSED_PATH)

    

    logger.info(f"[feature_engineering] Finished Job 2.")

if __name__ == "__main__":
    run_feature_engineering()
