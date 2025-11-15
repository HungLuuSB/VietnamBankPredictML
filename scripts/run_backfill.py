import sys
from pathlib import Path
from datetime import date

from loguru import logger

BASE_DIR = Path(__file__).resolve().parent.parent
sys.path.append(str(BASE_DIR))

from utils.config_loader import config
from pyspark.sql import SparkSession
from pyspark.sql.functions import regexp_replace, col
from pyspark.sql.types import LongType, StructType, StructField, StringType, DoubleType
from collectors.collectors import VnStockCollector, YFinanceCollector


def main():
    BASE_PATH = config["infrastructure"]["hdfs_base_path"]
    RAW_PATH = f"{BASE_PATH}/raw_ticks/"
    START_DATE = "2010-01-01"
    END_DATE = date.today().isoformat()

    VN_STOCKS = config["project"]["vn_stocks"]
    GLOBAL_ASSETS = config["project"]["global_assets"]
    POSTGRES_URL = config["infrastructure"]["postgres_url"]
    POSTGRES_USER = config["infrastructure"]["postgres_user"]
    POSTGRES_PASSWORD = config["infrastructure"]["postgres_pass"]
    logger.info(f"[backfill] Starting backfill process from {START_DATE} to {END_DATE}...")
    spark: SparkSession = SparkSession.builder.appName("BackFill").getOrCreate() #type: ignore
    spark.sparkContext.setLogLevel("WARN")

    schema = StructType([
        StructField("date", StringType(), True),
        StructField("open", DoubleType(), True),
        StructField("high", DoubleType(), True),
        StructField("low", DoubleType(), True),
        StructField("close", DoubleType(), True),
        StructField("volume", LongType(), True),
        StructField("symbol", StringType(), True)
    ])

    vn_col = VnStockCollector()
    yf_col = YFinanceCollector()

    def process_and_write(symbol: str, collector, source_type):
        logger.info(f"[backfill] Retrieving data for {symbol}...")
        pdf = collector.fetch_stock_data(symbol)
        pdf["symbol"] = symbol
        if pdf.empty:
            return

        try:
            df_spark = spark.createDataFrame(pdf, schema=schema)
        except Exception as e:
            logger.error(f"[backfill] Error while convertting schema for {symbol}: {e}")
            df_spark = spark.createDataFrame(pdf)
        
        df_spark_cleaned = df_spark.withColumn(
            "symbol", 
            regexp_replace(col("symbol"), "[^a-zA-Z0-9]", "_")
        )
        df_spark_cleaned = df_spark_cleaned.withColumn("date", col("date").cast(StringType()))
        df_spark_cleaned.show(10)

        df_spark_cleaned.write \
            .mode("append") \
            .partitionBy("symbol") \
            .parquet(RAW_PATH)
        
        df_spark_cleaned.write \
            .format("jdbc") \
            .option("driver", "org.postgresql.Driver") \
            .option("url", POSTGRES_URL) \
            .option("dbtable", "stock_db") \
            .option("user", POSTGRES_USER) \
            .option("password", POSTGRES_PASSWORD) \
            .mode("overwrite") \
            .save()

        logger.info(f"[backfill] Finished writing data for {symbol}")


    for symbol in VN_STOCKS:
        process_and_write(symbol, vn_col, "VN")

    for symbol in GLOBAL_ASSETS:
        process_and_write(symbol, yf_col, "Global")

    logger.success(f"[backfill] Finished backfilling.")
    spark.stop()

if __name__ == "__main__":
    main()
