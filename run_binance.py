import argparse
import os
os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = os.path.join(os.getcwd(), 'credential.json')
import time, datetime, threading
import us_finance_streaming_data_miner.util.logging as logging
from us_finance_streaming_data_miner.ingest.streaming.binance_run import BinanceAggregationsRun

def run(subscription_id, shard_id, shard_size):
    logging.info('starting the job: {dt_str}'.format(dt_str=datetime.datetime.now()))
    _ = BinanceAggregationsRun(shard_id = shard_id, shard_size = shard_size, subscription_id = subscription_id)

def log_heartbeat():
    while True:
        logging.info("us_finance_streaming_data_miner: heartbeat message.")
        time.sleep(30 * 60)

if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    pubsub_id_default = os.getenv('BINANCE_STREAM_INTRADAY_PUBSUB_SUBSCRIPTION_ID')
    parser.add_argument("-s", "--subscription_id", default=pubsub_id_default, help="pubsub subscription id to read the stream from.")
    parser.add_argument("-i", "--shard_id", default=0, help="zero-based shard id.")
    parser.add_argument("-z", "--shard_size", default=1, help="total number of shards.")
    args = parser.parse_args()

    threading.Thread(target=log_heartbeat).start()
    run(args.subscription_id, args.shard_id, args.shard_size)
