import psycopg2
from psycopg2.extensions import connection
from urllib.parse import urlparse
from loguru import logger
from src.utils.config_loader import config

class DatabaseConnector:
    def __init__(self):
        self.db_conf = config["database"]

    def get_postgress_connection(self) -> connection:
        try:
            jdbc_url = self.db_conf['postgres_url']
            clean_url = jdbc_url.replace("jdbc:", "") 
            parsed = urlparse(clean_url)
            db_host = parsed.hostname
            db_port = parsed.port or 5432
            db_name = parsed.path.lstrip("/")
            conn = psycopg2.connect(
                host=db_host,
                port=db_port,
                dbname=db_name,
                user=self.db_conf['postgres_user'],
                password=self.db_conf['postgres_pass'],
                connect_timeout=10
            )
            conn.autocommit = True 
            logger.debug(f"[DBUtils] Connected to PostgreSQL: {db_name}@{db_host}")
            return conn
        except Exception as e:
            logger.critical(f"[DBUtils] Failed to connect to PostgreSQL: {e}")
            raise

db_connector = DatabaseConnector()
