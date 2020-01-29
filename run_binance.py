import argparse
import os
os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = os.path.join(os.getcwd(), 'credential.json')
import time, datetime, threading
import us_finance_streaming_data_miner.util.logging as logging
from us_finance_streaming_data_miner.ingest.streaming.binance_run import BinanceAggregationsRun

def run():
    logging.info('starting the job: {dt_str}'.format(dt_str=datetime.datetime.now()))
    _ = BinanceAggregationsRun(subscription_id = os.getenv('BINANCE_STREAM_INTRADAY_PUBSUB_SUBSCRIPTION_ID'))

def log_heartbeat():
    while True:
        logging.info("us_finance_streaming_data_miner: heartbeat message.")
        time.sleep(30 * 60)

if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    args = parser.parse_args()

    threading.Thread(target=log_heartbeat).start()
    run()
