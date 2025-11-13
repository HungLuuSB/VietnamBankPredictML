from loguru import logger
import schedule
import time
import sys
from pathlib import Path

sys.path.append(str(Path(__file__).resolve().parent.parent))
from utils.config_loader import config
from collectors.collectors import VnStockCollector, YFinanceCollector
from streaming.kafka_producer import KafkaDataProducer


VN_STOCKS = config["project"]["vn_stocks"]
GLOBAL_ASSETS = config["project"]["global_assets"]

POLL_INTERVAL = config['producer']['poll_interval']
KAFKA_TOPIC = config['producer']['kafka_topic']
KAFKA_SERVER = config['infrastructure']['kafka_server']

global_state_memory = {}

vn_col = VnStockCollector()
yf_col = YFinanceCollector()

producer = KafkaDataProducer(bootstrap_servers=KAFKA_SERVER)

def check_and_send(symbol: str, collector_obj):
    symbol = symbol.upper()
    logger.info(f"[polling] Polling data for {symbol}")
    df_new = collector_obj.fetch_stock_data(symbol)
    if df_new.empty:
        return

    try:
        new_data_dict = df_new.iloc[-1].to_dict()
        new_data_dict["symbol"] = symbol
        new_price = new_data_dict.get('close')
    except (IndexError, AttributeError):
        logger.error(f"[polling] Invalid data for {symbol}. Skipped.")
        return

    last_price = global_state_memory.get(symbol)

    if new_price != last_price:
        logger.debug(f"[polling] New: {new_price} | Old: {last_price}")
        success = producer.send_message(
            topic=KAFKA_TOPIC,
            data=new_data_dict
        )
        if success:
            global_state_memory[symbol] = new_price

def job_check_data_changes():
    logger.info(f"[JOB RUNNING @ {time.strftime('%Y-%m-%d %H:%M:%S')}]")

    for symbol in VN_STOCKS:
        check_and_send(symbol, vn_col)

    for symbol in GLOBAL_ASSETS:
        check_and_send(symbol, yf_col)

def run_scheduler():
    logger.info(f"[polling/scheduler] Job run each {POLL_INTERVAL} seconds.")
    schedule.every(POLL_INTERVAL).seconds.do(job_check_data_changes)
    job_check_data_changes()

    try:
        while True:
            schedule.run_pending()
            time.sleep(1)
    except KeyboardInterrupt:
        producer.close()
        logger.info(f"[polling/scheduler] Job stopped.")

if __name__ == "__main__":
    run_scheduler()
