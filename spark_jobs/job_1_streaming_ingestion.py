import sys
from pathlib import Path

BASE_DIR = Path(__file__).resolve().parent.parent
sys.path.append(str(BASE_DIR))
from pathlib import Path

from loguru import logger
from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, regexp_replace
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, TimestampType, LongType
from utils.config_loader import config
def run_streaming_ingestion():
    infra = config['infrastructure']
    kafka_cfg = config['producer']

    KAFKA_SERVER = str(infra['kafka_server']).strip()
    KAFKA_TOPIC = str(kafka_cfg['kafka_topic']).strip()
    BASE_PATH = str(infra['hdfs_base_path']).strip()
    RAW_PATH = f"{BASE_PATH}/raw_ticks/"
    CHECKPOINT_PATH = f"{BASE_PATH}/checkpoints/kafka_ingestion/"
    CHECKPOINT_PATH_CONSOLE = f"{BASE_PATH}/checkpoints/kafka_ingestion_console/"

    logger.info(f"[streaming_ingestion] Kafka Server: {KAFKA_SERVER}\nTopic: {KAFKA_TOPIC}\n HDFS: {RAW_PATH}\n Checkpoint: {CHECKPOINT_PATH}")

    schema = StructType([
        StructField("date", StringType(), True),
        StructField("open", DoubleType(), True),
        StructField("high", DoubleType(), True),
        StructField("low", DoubleType(), True),
        StructField("close", DoubleType(), True),
        StructField("volume", LongType(), True),
        StructField("symbol", StringType(), True)
    ])

    spark: SparkSession = SparkSession.builder.appName("DataIngestion").getOrCreate() #type: ignore
    spark.sparkContext.setLogLevel("WARN")

    df_kafka = (
        spark.readStream.format("kafka")
        .option("kafka.bootstrap.servers", KAFKA_SERVER)
        .option("subscribe", KAFKA_TOPIC)
        .option("startingOffsets", "latest")
        .load()
    )

    df_parsed = df_kafka.select(
        from_json(col("value").cast("string"), schema).alias("data")
    ).select("data.*")
    
    df_cleaned = df_parsed.withColumn(
        "symbol", 
        regexp_replace(col("symbol"), "[^a-zA-Z0-9]", "_")
    )

    query = (
        df_cleaned.writeStream.format("parquet")
        .outputMode("append")
        .option("path", RAW_PATH)
        .option("checkpointLocation", CHECKPOINT_PATH)
        .partitionBy("symbol")
        .start()
    )

    query_to_console = df_cleaned.writeStream \
        .format("console") \
        .outputMode("append") \
        .option("checkpointLocation", CHECKPOINT_PATH_CONSOLE) \
        .start()

    logger.info(f"[Job 1] Running... (Waiting for data from Kafka)")
    query.awaitTermination()
    query_to_console.stop()

if __name__ == "__main__":
    run_streaming_ingestion()
