import argparse
import os
os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = os.path.join(os.getcwd(), 'credential.json')
import time
import us_finance_streaming_data_miner.history.history
import us_finance_streaming_data_miner.util.time
import config
import us_finance_streaming_data_miner.util.logging as logging
from us_finance_streaming_data_miner.ingest.streaming.polygon_run import PolygonAggregationsMockRun


def run():
    polygon_run = PolygonAggregationsMockRun()

    while True:
        polygon_run.on_daily_trade_start()

        time.sleep(10)

        polygon_run.on_daily_trade_end()

        time.sleep(20)

if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    args = parser.parse_args()
    run()
