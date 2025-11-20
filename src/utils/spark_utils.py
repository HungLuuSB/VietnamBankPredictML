from pyspark.sql import SparkSession
from pyspark.sql.types import LongType, StructType, StructField, StringType, DoubleType, DateType
from loguru import logger
from src.utils.config_loader import config

STOCK_SCHEMA = StructType([
    StructField("date", StringType(), True),
    StructField("open", DoubleType(), True),
    StructField("high", DoubleType(), True),
    StructField("low", DoubleType(), True),
    StructField("close", DoubleType(), True),
    StructField("volume", LongType(), True),
    StructField("symbol", StringType(), True)
])

def get_spark_session(app_name: str) -> SparkSession:
    try:
        logger.info(f"[SparkUtils] Initializing SparkSession for: {app_name}")
        spark_conf = config.get('spark', {})
        master = spark_conf.get('master', 'local[*]')
        packages = spark_conf.get('packages', '')

        builder = SparkSession.builder.appName(app_name).master(master).config("spark.sql.session.timeZone", "Asia/Ho_Chi_Minh").config("spark.sql.caseSensitive", "false") #type: ignore

        if packages:
            builder = builder.config("spark.jars.packages", packages)
            logger.debug(f"[SparkUtils] Loading packages: {packages}")

        spark = builder.getOrCreate()
        spark.sparkContext.setLogLevel("ERROR")
        logger.success(f"[SparkUtils] SparkSession '{app_name}' started successfully.")
        return spark
    except Exception as e:
        logger.critical(f"[SparkUtils] Failed to initialize SparkSession: {e}")
        raise RuntimeError("Could not start Spark. Check Java/Hadoop installation.") from e
