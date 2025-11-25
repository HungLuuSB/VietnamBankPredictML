#spark-submit     --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0,org.postgresql:postgresql:42.6.0     src/jobs/job_1_ingestion.py
#psql -U postgres
import sys
import re
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
        db_conf = config['database']

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

    def process_batch(df, epoch_id):
        logger.info(f"--- Processing Batch ID: {epoch_id} ---")
        df.cache()
        count = df.count()
        if count == 0:
            df.unpersist()
            return
        try:
            logger.debug(f"Writing {count} rows to Data Lake...")
            df.write \
                .mode("append") \
                .partitionBy("symbol") \
                .parquet(RAW_PATH)
        except Exception as e:
            logger.error(f"Failed to write to Data Lake: {e}")

        try:
            stocks_in_batch = [row.symbol for row in df.select("symbol").distinct().collect()]
            jdbc_url = db_conf['postgres_url']
            db_props = {
                "user": db_conf['postgres_user'],
                "password": db_conf['postgres_pass'],
                "driver": db_conf['jdbc_driver_path']
            }

            logger.debug(f"Syncing batch to PostgreSQL for stocks: {stocks_in_batch}")

            for stock in stocks_in_batch:
                table_name = re.sub(r"[^a-zA-Z0-9]", "", stock).lower()
                df_stock = df.filter(col("symbol") == stock)
                df_export = df_stock.select(
                    "date", "open", "high", "low", "close", "volume"
                )
                df_export.write.jdbc(
                    url=jdbc_url,
                    table=table_name,
                    mode="append",
                    properties=db_props
                )
        except Exception as e:
            logger.error(f"Failed to write to PostgreSQL: {e}")
        df.unpersist()
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
        .foreachBatch(process_batch) \
        .option("path", RAW_PATH) \
        .option("checkpointLocation", CHECKPOINT_PATH) \
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
