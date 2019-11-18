import yaml, datetime
from pytz import timezone


def load(filename):
    with open(filename, 'r') as ymlfile:
        cfg = yaml.load(ymlfile)
    return cfg

def get_tz(cfg):
    return timezone(cfg['schedule']['timezone'])

def _get_tz_utcoffset_hours(cfg):
    tz = timezone(cfg['schedule']['timezone'])
    td = tz.utcoffset(datetime.datetime.utcnow())
    return td.seconds // 3600

def get_daily_ingestion_start_t(cfg):
    t = datetime.datetime.strptime(cfg['schedule']['daily_ingestion_start'], '%H:%M:%S')
    return datetime.time(t.hour, t.minute, t.second, tzinfo=get_tz(cfg))

def get_daily_last_record_ingestion_start_t(cfg):
    t = datetime.datetime.strptime(cfg['schedule']['daily_last_record_ingestion_start'], '%H:%M:%S')
    return datetime.time(t.hour, t.minute, t.second, tzinfo=get_tz(cfg))

def get_intraday_ingestion_start_t(cfg):
    t = datetime.datetime.strptime(cfg['schedule']['intraday_ingestion_start'], '%H:%M:%S')
    return datetime.time(t.hour, t.minute, t.second, tzinfo=get_tz(cfg))

def get_log_name(cfg):
    return cfg['logname']

def get_uploadname(cfg):
    return cfg['uploadname']
