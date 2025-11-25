#spark-submit src/jobs/job_0_producer.py
import time
import schedule
import sys
from pathlib import Path


sys.path.append(str(Path(__file__).resolve().parents[2]))

from loguru import logger
from src.utils.config_loader import config
from src.utils.kafka_producer import KafkaClient
from src.collectors import VnStockCollector
from src.collectors import YFinanceCollector
from src.collectors import VietFinCollector

last_known_prices = {}

def fetch_and_send(symbol: str, collector, producer: KafkaClient):
    try:
        df = collector.fetch_data(symbol)
        if df.empty:
            logger.warning(f"[{symbol}] No data found currently.")
            return

        record = df.iloc[-1].to_dict()
        current_price = record.get('close')
        last_price = last_known_prices.get(symbol)
        if current_price != last_price:
            logger.info(f"[{symbol}] Price changed: {last_price} -> {current_price}. Sending to Kafka...")
            success = producer.send(record)
            if success:
                last_known_prices[symbol] = current_price
        else:
            logger.debug(f"[{symbol}] No change ({current_price}). Skipping.")
    except Exception as e:
        logger.error(f"[{symbol}] Pipeline error: {e}")

def job_cycle():
    logger.info("--- Starting Polling Cycle ---")
    producer = KafkaClient()
    #vn_col = VnStockCollector()
    vn_col = VietFinCollector()
    yf_col = YFinanceCollector()
    targets = config['project']['target_stocks']
    for stock in targets:
        fetch_and_send(stock, vn_col, producer)
    globals = config['project']['global_assets']
    for asset in globals:
        fetch_and_send(asset, yf_col, producer)
    producer.close()
    logger.info("--- Cycle Finished ---")

def run():
    seconds = 10
    logger.info(f"[Job 0] Producer started. Polling every {seconds} minutes.")
    job_cycle()
    schedule.every(seconds).seconds.do(job_cycle)
    try:
        while True:
            schedule.run_pending()
            time.sleep(1)
    except KeyboardInterrupt:
        logger.info("[Job 0] Stopping producer...")

if __name__ == "__main__":
    run()
