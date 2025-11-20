import sys
from pathlib import Path

BASE_DIR = Path(__file__).resolve().parents[2]
sys.path.append(str(BASE_DIR))

from pyspark.sql.functions import from_json, col, regexp_replace, date_format
from pyspark.sql.types import StringType
from loguru import logger

from src.utils.spark_utils import get_spark_session, STOCK_SCHEMA
from src.utils.config_loader import config

def run():
    logger.info("[Ingestion] Initializing Streaming Ingestion Pipeline...")
    spark = get_spark_session("Ingestion_Streaming")
    try:
        kafka_conf = config['kafka']
        storage_conf = config['storage']

        KAFKA_SERVER = kafka_conf['server']
        KAFKA_TOPIC = kafka_conf['topic']
        BASE_PATH = storage_conf['hdfs_base']
        RAW_PATH = f"{BASE_PATH}/{storage_conf['raw_zone']}"
        CHECKPOINT_PATH = f"{BASE_PATH}/{storage_conf['checkpoint_zone']}/ingestion"
        logger.info(f"[Ingestion] Configuration loaded. Kafka Server: {KAFKA_SERVER}, Topic: {KAFKA_TOPIC}")
        logger.info(f"[Ingestion] Data Lake Path: {RAW_PATH}")
    except KeyError as e:
        logger.critical(f"[Ingestion] Configuration error. Missing key: {e}")
        sys.exit(1)
    try:
        df_stream = spark.readStream \
            .format("kafka") \
            .option("kafka.bootstrap.servers", KAFKA_SERVER) \
            .option("subscribe", KAFKA_TOPIC) \
            .option("startingOffsets", "latest") \
            .option("failOnDataLoss", "false") \
            .load()
        logger.info("[Ingestion] Successfully connected to Kafka broker.")
    except Exception as e:
        logger.critical(f"[Ingestion] Failed to connect to Kafka. Error: {e}")
        sys.exit(1)
    df_parsed = df_stream.select(
        from_json(col("value").cast("string"), STOCK_SCHEMA).alias("data")
    ).select("data.*")

    df_cleaned = df_parsed.withColumn(
        "symbol", 
        regexp_replace(col("symbol"), "[^a-zA-Z0-9]", "_")
    )

    #df_final = df_cleaned.withColumn("date", col("date").cast(StringType()))
    df_final = df_cleaned.withColumn("date", date_format(col("date"), "yyyy-MM-dd"))
    query = df_final.writeStream \
        .format("parquet") \
        .outputMode("append") \
        .option("path", RAW_PATH) \
        .option("checkpointLocation", CHECKPOINT_PATH) \
        .partitionBy("symbol") \
        .start()

    query_console = df_cleaned.writeStream \
        .format("console") \
        .outputMode("append") \
        .option("truncate", "false") \
        .start()

    logger.info("[Ingestion] Pipeline started. Waiting for incoming data streams...")
    try:
        spark.streams.awaitAnyTermination()
    except KeyboardInterrupt:
        logger.warning("[Ingestion] Process stopped by user (KeyboardInterrupt).")
        query.stop()
    except Exception as e:
        logger.critical(f"[Ingestion] Streaming query failed unexpectedly. Error: {e}")
        query.stop()
        sys.exit(1)

if __name__ == "__main__":
    run()
