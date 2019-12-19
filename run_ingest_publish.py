import argparse
import os
os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = os.path.join(os.getcwd(), 'credential.json')
import time
import us_finance_streaming_data_miner.history.history
import us_finance_streaming_data_miner.util.time
import config
import us_finance_streaming_data_miner.util.logging as logging
from us_finance_streaming_data_miner.ingest.streaming.polygon_run import PolygonAggregationsRun
import us_finance_streaming_data_miner.upload.daily as daily_upload


def run(forcerun):
    cfg = config.load('config.us.yaml')
    tz = config.get_tz(cfg)
    polygon_run = PolygonAggregationsRun()

    while True:
        dt = us_finance_streaming_data_miner.util.time.get_utcnow().astimezone(tz)
        dt_str = str(dt.date())

        if dt.weekday() >= 5:
            logging.info('skipping the routing during weekend, weekday: {weekday} for {dt_str}'.format(
                weekday=dt.weekday(), dt_str=dt_str))
            time.sleep(60 * 60)
            continue


        logging.info('checking if run for {date_str} should be done'.format(date_str=dt_str))
        if not forcerun and us_finance_streaming_data_miner.history.history.did_run_today(cfg):
            logging.info('run for {date_str} is already done'.format(date_str=dt_str))
            time.sleep(10 * 60)
            continue

        t_market_open = config.get_market_open(cfg)
        while True:
            t_cur = us_finance_streaming_data_miner.util.time.get_utcnow().astimezone(tz).time()
            logging.info(cfg, 'checking if the schedule time for {dt_str} has reached'.format(dt_str=dt_str))
            if forcerun or t_cur > t_market_open:
                polygon_run.on_daily_trade_start()
                break

            logging.info(cfg, 'schedule time {t_run_after} not yet reached at {t_cur}'.format(t_run_after=t_market_open, t_cur=t_cur))
            time.sleep(10 * 60)

        if forcerun:
            time.sleep(70)

        t_ingest_end = config.get_market_ingest_end(cfg)
        while True:
            t_cur = us_finance_streaming_data_miner.util.time.get_utcnow().astimezone(tz).time()
            logging.info(cfg, 'checking if the schedule time for {dt_str} has reached'.format(dt_str=dt_str))
            logging.info(polygon_run.get_status_string())
            if forcerun or t_cur > t_ingest_end:
                polygon_run.save_daily_df()
                daily_upload.upload()

                polygon_run.on_daily_trade_end()
                us_finance_streaming_data_miner.history.history.on_run(cfg)
                break

            logging.info(cfg, 'schedule time {t_run_after} not yet reached at {t_cur}'.format(t_run_after=t_ingest_end, t_cur=t_cur))
            time.sleep(10 * 60)

        if forcerun:
            # forcerun runs only once
            break


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument("-f", "--forcerun", action="store_true", help="forces run without waiting without observing the schedule.")
    args = parser.parse_args()

    if args.forcerun:
        print('forcerun on')
    run(args.forcerun)
