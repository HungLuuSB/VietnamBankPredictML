import json
from loguru import logger
from kafka import KafkaProducer
from kafka.errors import KafkaError

class KafkaDataProducer():
    def __init__(self, bootstrap_servers: str = "localhost:9092") -> None:
        try:
            self.producer = KafkaProducer(
                bootstrap_servers=bootstrap_servers,
                value_serializer=lambda v: json.dumps(v, default=str).encode('utf-8')
            )
            logger.info(f"[KafkaProducer] Connected successfully.")
        except KafkaError as e:
            logger.error(f"[KafkaProducer] Error while connecting: {e}")
            self.producer = None

    def send_message(self, topic: str, data: dict):
        if not self.producer:
            logger.error(f"[KafkaProducer] Producer is not connected.")
            return False
        try:
            future = self.producer.send(topic, value=data)
            record_metadata = future.get(timeout=10)
            logger.info(f"[KafkaProducer] Sent message to topic: {record_metadata.topic}")
            return True
        except Exception as e:
            logger.error(f"[KafkaProducer] Error while sending message: {e}")
            return False

    def close(self):
        if self.producer:
            self.producer.close()
            logger.info(f"[KafkaProducer] Producer has been closed.")
