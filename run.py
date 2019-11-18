import argparse

import os
os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = os.path.join(os.getcwd(), 'credential.json')

import time
import util.time
import config
import ingest.daily.quandl
import ingest.combine
import util.logging as logging


def run_download(cfg):
    logging.info(cfg, 'downloading us daily')
    download.download.download(download.download.DEST_DIR_DAILY)
    logging.info(cfg, 'download complete')

def run_ingests_append_combine():
    ingest.daily.quandl.download_histories_csv(2)
    ingest.combine.combine_and_save_files('data/daily', ['date', 'symbol'])

def run_upload(cfg):
    logging.info(cfg, 'uploading us daily')
    upload.daily.upload.upload(cfg)
    logging.info(cfg, 'uplaod complete')

def run(forcerun):
    cfg = config.load('config.us.yaml')
    tz = config.get_tz(cfg)

    while True:
        dt_str = str(util.time.get_utcnow().astimezone(tz).date())
        logging.info(cfg, 'checking if run for {dt_str} should be done'.format(dt_str=dt_str))
        if not forcerun and upload.daily.history.did_upload_today():
            logging.info(cfg, 'run for {dt_str} is already done'.format(dt_str=dt_str))
            time.sleep(10 * 60)
            continue

        t_run_after = config.get_daily_ingestion_start_t(cfg)
        while True:
            t_cur = util.time.get_utcnow().astimezone(tz).time()
            logging.info(cfg, 'checking if the schedule time for {dt_str} has reached'.format(dt_str=dt_str))
            if forcerun or t_cur > t_run_after:
                run_download(cfg)
                run_ingests_append_combine()
                run_upload(cfg)
                upload.daily.history.on_upload()
                break

            logging.info(cfg, 'schedule time {t_run_after} not yet reached at {t_cur}'.format(t_run_after=t_run_after, t_cur=t_cur))
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
