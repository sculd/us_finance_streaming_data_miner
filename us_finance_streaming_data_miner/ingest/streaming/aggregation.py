import pandas as pd, numpy as np
import datetime, os
import pytz
import us_finance_streaming_data_miner.util.logging as logging

from enum import Enum

_TIME_ZONE_US_EASTERN = 'US/Eastern'

class BAR_INTERVAL(Enum):
    ONE_MINUTE = 1

class Trade:
    def __init__(self, timestamp_seconds, symbol, price, volume):
        self.timestamp_seconds, self.symbol, self.price, self.volume = timestamp_seconds, symbol, price, volume

class Bar:
    def __init__(self, symbol, open_, high, low, close_, volume):
        self.symbol, self.open, self.high, self.low, self.close, self.volume = symbol, open_, high, low, close_, volume

    def new_bar_with_trade(symbol, price, volume):
        return Bar(symbol, price, price, price, price, volume)

    def on_trade(self, trade):
        if self.symbol != trade.symbol:
            raise Exception('symbol mismatch')
        self.high = max(self.high, trade.price)
        self.low = min(self.low, trade.price)
        self.close = trade.price
        self.volume += trade.volume

    @staticmethod
    def get_tuple_names():
        return ('symbol', 'open', 'high', 'low', 'close', 'volume',)

    def to_tuple(self):
        return (self.symbol, self.open, self.high, self.low, self.close, self.volume, )

class BarWithTime:
    def truncate_to_minute(timestamp_seconds):
        t = datetime.datetime.utcfromtimestamp(timestamp_seconds)
        t_tz = pytz.utc.localize(t)
        t_tz_minute = t_tz.replace(second=0, microsecond=0)
        return t_tz_minute

    def __init__(self, time, bar):
        '''

        :param time: datetime instance in utc timezone
        :param bar:
        '''
        self.time = time
        self.bar = bar

    def get_next_bar_time(self):
        return self.time + datetime.timedelta(minutes=1)

    @staticmethod
    def get_minute_tuple_names():
        return ('datetime',) + Bar.get_tuple_names()

    @staticmethod
    def get_daily_tuple_names():
        return ('date',) + Bar.get_tuple_names()

    def to_tuple(self):
        return (self.time,) + self.bar.to_tuple()

class Aggregation:
    def __init__(self, symbol):
        self.symbol = symbol
        self.bar_with_times = []
        self._bar_with_times_max_length = 300
        self.t_now_tz = None

    def set_now_tz(self, now_tz):
        self.t_now_tz = now_tz

    def _get_t_now_tz(self):
        if self.t_now_tz:
            return self.t_now_tz
        t_now = datetime.datetime.utcnow()
        return pytz.utc.localize(t_now)

    def _on_first_trade(self, trade):
        assert self.symbol == trade.symbol
        bar_timestamped = BarWithTime(BarWithTime.truncate_to_minute(trade.timestamp_seconds), Bar.new_bar_with_trade(trade.symbol, trade.price, 0))
        self.bar_with_times.append(bar_timestamped)
        self._on_append_new_bar_with_time()

    def _on_first_bar_with_time(self, bar_with_time):
        assert self.symbol == bar_with_time.bar.symbol
        bar_timestamped = BarWithTime(bar_with_time.time, bar_with_time.bar)
        self.bar_with_times.append(bar_timestamped)
        self._on_append_new_bar_with_time()

    def _new_bar_with_zero_volume(self, t, price):
        bar_timestamped = BarWithTime(t, Bar.new_bar_with_trade(self.symbol, price, 0))
        self.bar_with_times.append(bar_timestamped)
        self._on_append_new_bar_with_time()

    def _on_append_new_bar_with_time(self):
        if len(self.bar_with_times) > self._bar_with_times_max_length:
            self.bar_with_times = self.bar_with_times[-self._bar_with_times_max_length:]

    def on_trade(self, trade):
        assert self.symbol == trade.symbol
        if not self.bar_with_times:
            self._on_first_trade(trade)

        trade_t = BarWithTime.truncate_to_minute(trade.timestamp_seconds)
        while True:
            bar_with_time = self.bar_with_times[-1]
            if bar_with_time.time == trade_t:
                break

            time = bar_with_time.get_next_bar_time()
            price = bar_with_time.bar.close
            if time == trade_t:
                price = trade.price
            self._new_bar_with_zero_volume(time, price)

        bar_with_time = self.bar_with_times[-1]
        assert bar_with_time.time == trade_t
        bar_with_time.bar.on_trade(trade)

    def on_bar_with_time(self, new_bar_with_time):
        assert self.symbol == new_bar_with_time.bar.symbol
        if not self.bar_with_times:
            self._on_first_bar_with_time(new_bar_with_time)

        bar_t = new_bar_with_time.time
        cnt = 0
        while True:
            cnt += 1
            if cnt > 100:
                print('breaking after more than {cnt} loops'.format(cnt=cnt))
                break
            bar_with_time = self.bar_with_times[-1]
            if bar_with_time.time >= bar_t:
                break

            time = bar_with_time.get_next_bar_time()
            price = bar_with_time.bar.close
            if time == bar_t:
                price = bar_with_time.bar.open
            self._new_bar_with_zero_volume(time, price)

        if len(self.bar_with_times) == 0:
            print('bar_with_times has no elements')

        inserted_bar = self.bar_with_times[-1].bar
        inserted_bar.open = new_bar_with_time.bar.open
        inserted_bar.high = new_bar_with_time.bar.high
        inserted_bar.low = new_bar_with_time.bar.low
        inserted_bar.close = new_bar_with_time.bar.close
        inserted_bar.volume = new_bar_with_time.bar.volume

class Aggregations:
    def __init__(self):
        self.aggregation_per_symbol = {}

    def clean(self):
        self.aggregation_per_symbol = {}

    def on_trade(self, trade):
        if trade.symbol not in self.aggregation_per_symbol:
            self.aggregation_per_symbol[trade.symbol] = Aggregation(trade.symbol)
        self.aggregation_per_symbol[trade.symbol].on_trade(trade)

    def on_bar_with_time(self, bar_with_time):
        symbol = bar_with_time.bar.symbol
        if symbol not in self.aggregation_per_symbol:
            self.aggregation_per_symbol[symbol] = Aggregation(symbol)
        self.aggregation_per_symbol[symbol].on_bar_with_time(bar_with_time)

    def get_status_string(self):
        bars_avg = np.mean(list(map(lambda ag: len(ag.bar_with_times), self.aggregation_per_symbol.values())))
        return 'size of aggregation_per_symbol: {l}, bars_avg: {bars_avg}'.format(
            l = len(self.aggregation_per_symbol),
            bars_avg = bars_avg
        )

class AggregationsRun:
    def __init__(self, aggregations = None):
        self.aggregations = aggregations if aggregations else Aggregations()
        self.daily_trade_started = True

    def print_msg(self, msg):
        print('[print_msg]', msg)

    def on_trade(self, trade):
        if self.daily_trade_started:
            self.aggregations.on_trade(trade)

    def on_bar_with_time(self, bar_with_time):
        if self.daily_trade_started:
            self.aggregations.on_bar_with_time(bar_with_time)

    def on_daily_trade_start(self):
        logging.info('on_daily_trade_start')
        self.daily_trade_started = True

    def save_daily_df(self, base_dir='data'):
        pass

    def on_daily_trade_end(self, base_dir='data'):
        logging.info('on_daily_trade_end')
        self.daily_trade_started = False
        self.aggregations.clean()

    def get_status_string(self):
        return self.aggregations.get_status_string()
