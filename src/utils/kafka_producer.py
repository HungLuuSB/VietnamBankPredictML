import json
from kafka import KafkaProducer
from kafka.errors import KafkaError
from loguru import logger
from src.utils.config_loader import config

class KafkaClient:
    def __init__(self):
        self.bootstrap_server = config['kafka']['server']
        self.topic = config['kafka']['topic']
        self.producer = None
        self._connect()

    def _connect(self):
        try:
            self.producer = KafkaProducer(
                bootstrap_servers=self.bootstrap_server,
                value_serializer=lambda v: json.dumps(v, default=str).encode('utf-8'),
                linger_ms=10
            )
            logger.success(f"[Kafka] Connected to {self.bootstrap_server}")
        except KafkaError as e:
            logger.critical(f"[Kafka] Connection failed: {e}")

    def send(self, data: dict):
        if not self.producer:
            self._connect()
            if not self.producer: return False

        try:
            future = self.producer.send(self.topic, value=data)
            meta = future.get(timeout=5)
            logger.info(f"[Kafka] Sent {data.get('stock_code')} to {meta.topic}-{meta.partition}@{meta.offset}")
            return True
        except Exception as e:
            logger.error(f"[Kafka] Send error: {e}")
            return False

    def close(self):
        if self.producer:
            self.producer.close()
