from collections import defaultdict
import pandas as pd
import datetime
import pytz

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
        self.time = time
        self.bar = bar

    def get_next_bar_time(self):
        return self.time + datetime.timedelta(minutes=1)

    @staticmethod
    def get_tuple_names():
        return ('datetime',) + Bar.get_tuple_names()

    def to_tuple(self):
        return (self.time,) + self.bar.to_tuple()

class Aggregation:
    def __init__(self, symbol):
        self.symbol = symbol
        self.bar_with_times = []

    def _on_first_trade(self, trade):
        assert self.symbol == trade.symbol
        bar_timestamped = BarWithTime(BarWithTime.truncate_to_minute(trade.timestamp_seconds), Bar.new_bar_with_trade(trade.symbol, trade.price, 0))
        self.bar_with_times.append(bar_timestamped)

    def _new_bar_with_zero_volume(self, t, price):
        bar_timestamped = BarWithTime(t, Bar.new_bar_with_trade(self.symbol, price, 0))
        self.bar_with_times.append(bar_timestamped)

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

    def get_minute_df(self):
        tuples = list(map(lambda b: b.to_tuple(), self.bar_with_times))
        return pd.DataFrame(tuples, columns = BarWithTime.get_tuple_names())

    def get_daily_df(self):
        df_minute = self.get_minute_df()
        df_daily = pd.DataFrame(columns = ['date', 'symbol', 'open', 'high', 'low', 'close', 'volume']).append(
            {
                'date': df_minute.datetime.dt.date.iloc[0],
                'symbol': df_minute.symbol.iloc[0],
                'open': df_minute.open.iloc[0],
                'high': df_minute.high.max(),
                'low': df_minute.low.min(),
                'close': df_minute.close.iloc[-1],
                'volume': df_minute.volume.sum()
            }, ignore_index=True)
        return df_daily

class Aggregations:
    def __init__(self):
        self.aggregations = {}

    def on_trade(self, trade):
        if trade.symbol not in self.aggregations:
            self.aggregations[trade.symbol] = Aggregation(trade.symbol)
        self.aggregations[trade.symbol].on_trade(trade)

    def get_minute_df(self):
        df = pd.DataFrame()
        for _, aggregation in self.aggregations.items():
            df_ = aggregation.get_minute_df()
            df = df.append(df_)
        return df

    def get_daily_df(self):
        df = pd.DataFrame()
        for _, aggregation in self.aggregations.items():
            df_ = aggregation.get_daily_df()
            df = df.append(df_)
        return df
