import unittest, datetime
import pytz

from us_finance_streaming_data_miner.ingest.streaming.aggregation import AggregationsRun, Aggregations, Aggregation, BarWithTime, Bar, Trade

class TestBar(unittest.TestCase):
    def test_bar_on_trade(self):
        symbol = 'DUMMY_SYMBOL'
        bar = Bar.new_bar_with_trade(symbol, 100, 1.0)
        self.assertEqual(100, bar.open)
        self.assertEqual(100, bar.high)
        self.assertEqual(100, bar.low)
        self.assertEqual(100, bar.close)
        self.assertEqual(1, bar.volume)

        bar.on_trade(Trade(0, symbol, 110.0, 1.0))
        self.assertEqual(100, bar.open)
        self.assertEqual(110, bar.high)
        self.assertEqual(100, bar.low)
        self.assertEqual(110, bar.close)
        self.assertEqual(2, bar.volume)

        bar.on_trade(Trade(0, symbol, 90.0, 1.0))
        self.assertEqual(100, bar.open)
        self.assertEqual(110, bar.high)
        self.assertEqual(90, bar.low)
        self.assertEqual(90, bar.close)
        self.assertEqual(3, bar.volume)

class TestAggregation(unittest.TestCase):
    def test_on_trade(self):
        one_minute_seconds = 60
        symbol = 'DUMMY_SYMBOL'
        aggregation = Aggregation(symbol)
        aggregation.on_trade(Trade(0, symbol, 100.0, 1.0))
        aggregation.on_trade(Trade(1, symbol, 90.0, 1.0))
        aggregation.on_trade(Trade(2, symbol, 100.0, 1.0))
        aggregation.on_trade(Trade(3, symbol, 120.0, 1.0))
        aggregation.on_trade(Trade(4, symbol, 110.0, 1.0))

        aggregation.on_trade(Trade(one_minute_seconds, symbol, 110.0, 1.0))
        # skip minute 2
        aggregation.on_trade(Trade(one_minute_seconds * 3 + 1, symbol, 120.0, 1.0))
        aggregation.on_trade(Trade(one_minute_seconds * 3 + 2, symbol, 90.0, 1.0))

        self.assertEqual(4, len(aggregation.bar_with_times))

        bar_t_0 = aggregation.bar_with_times[0]
        self.assertEqual(bar_t_0.time.timestamp(), 0)
        self.assertEqual(symbol, bar_t_0.bar.symbol)
        self.assertEqual(100, bar_t_0.bar.open)
        self.assertEqual(120, bar_t_0.bar.high)
        self.assertEqual(90, bar_t_0.bar.low)
        self.assertEqual(110, bar_t_0.bar.close)
        self.assertEqual(5, bar_t_0.bar.volume)

        bar_t_1 = aggregation.bar_with_times[1]
        self.assertEqual(bar_t_1.time.timestamp(), one_minute_seconds)
        self.assertEqual(symbol, bar_t_1.bar.symbol)
        self.assertEqual(110, bar_t_1.bar.open)
        self.assertEqual(110, bar_t_1.bar.high)
        self.assertEqual(110, bar_t_1.bar.low)
        self.assertEqual(110, bar_t_1.bar.close)
        self.assertEqual(1, bar_t_1.bar.volume)

        bar_t_2 = aggregation.bar_with_times[2]
        self.assertEqual(bar_t_2.time.timestamp(), one_minute_seconds * 2)
        self.assertEqual(symbol, bar_t_2.bar.symbol)
        self.assertEqual(110, bar_t_2.bar.open)
        self.assertEqual(110, bar_t_2.bar.high)
        self.assertEqual(110, bar_t_2.bar.low)
        self.assertEqual(110, bar_t_2.bar.close)
        self.assertEqual(0, bar_t_2.bar.volume)

        bar_t_3 = aggregation.bar_with_times[3]
        self.assertEqual(bar_t_3.time.timestamp(), one_minute_seconds * 3)
        self.assertEqual(symbol, bar_t_3.bar.symbol)
        self.assertEqual(120, bar_t_3.bar.open)
        self.assertEqual(120, bar_t_3.bar.high)
        self.assertEqual(90, bar_t_3.bar.low)
        self.assertEqual(90, bar_t_3.bar.close)
        self.assertEqual(2, bar_t_3.bar.volume)

    def test_minute_df(self):
        one_minute_seconds = 60
        symbol = 'DUMMY_SYMBOL'
        aggregation = Aggregation(symbol)
        aggregation.bar_with_times.append(BarWithTime(BarWithTime.truncate_to_minute(0), Bar(symbol, 100, 120, 90, 110, 3)))
        aggregation.bar_with_times.append(BarWithTime(BarWithTime.truncate_to_minute(one_minute_seconds), Bar(symbol, 110, 130, 80, 130, 1)))
        df = aggregation.get_minute_df()
        self.assertEqual(2, len(df))
        row_0 = df.iloc[0]
        self.assertEqual(100, row_0.open)
        self.assertEqual(120, row_0.high)
        self.assertEqual(90, row_0.low)
        self.assertEqual(110, row_0.close)
        self.assertEqual(3, row_0.volume)
        row_1 = df.iloc[1]
        self.assertEqual(110, row_1.open)
        self.assertEqual(130, row_1.high)
        self.assertEqual(80, row_1.low)
        self.assertEqual(130, row_1.close)
        self.assertEqual(1, row_1.volume)

    def test_daily_df(self):
        one_minute_seconds = 60
        symbol = 'DUMMY_SYMBOL'
        aggregation = Aggregation(symbol)
        aggregation.bar_with_times.append(BarWithTime(BarWithTime.truncate_to_minute(0), Bar(symbol, 100, 120, 90, 110, 3)))
        aggregation.bar_with_times.append(BarWithTime(BarWithTime.truncate_to_minute(one_minute_seconds), Bar(symbol, 110, 130, 80, 130, 1)))
        df = aggregation.get_daily_df()
        self.assertEqual(1, len(df))
        row_0 = df.iloc[0]
        self.assertEqual(100, row_0.open)
        self.assertEqual(130, row_0.high)
        self.assertEqual(80, row_0.low)
        self.assertEqual(130, row_0.close)
        self.assertEqual(4, row_0.volume)

class TestAggregations(unittest.TestCase):
    def test_on_trade(self):
        aggregations = Aggregations()
        one_minute_seconds = 60
        symbol_1 = 'SYM1'
        symbol_2 = 'SYM2'

        aggregations.on_trade(Trade(0, symbol_1, 100.0, 1.0))
        self.assertEqual(1, len(aggregations.aggregation_per_symbol))

        aggregations.on_trade(Trade(0, symbol_2, 120.0, 1.0))
        self.assertEqual(2, len(aggregations.aggregation_per_symbol))

        aggregations.on_trade(Trade(1, symbol_1, 110.0, 1.0))
        aggregations.on_trade(Trade(one_minute_seconds, symbol_1, 120.0, 1.0))
        aggregations.on_trade(Trade(one_minute_seconds + 1, symbol_1, 130.0, 1.0))
        self.assertEqual(2, len(aggregations.aggregation_per_symbol))

        aggregation_1 = aggregations.aggregation_per_symbol[symbol_1]
        self.assertEqual(2, len(aggregation_1.bar_with_times))

        bar_1_t_0 = aggregation_1.bar_with_times[0]
        self.assertEqual(bar_1_t_0.time.timestamp(), 0)
        self.assertEqual(symbol_1, bar_1_t_0.bar.symbol)
        self.assertEqual(100, bar_1_t_0.bar.open)
        self.assertEqual(110, bar_1_t_0.bar.high)
        self.assertEqual(100, bar_1_t_0.bar.low)
        self.assertEqual(110, bar_1_t_0.bar.close)
        self.assertEqual(2, bar_1_t_0.bar.volume)

        bar_1_t_1 = aggregation_1.bar_with_times[1]
        self.assertEqual(bar_1_t_1.time.timestamp(), one_minute_seconds)
        self.assertEqual(symbol_1, bar_1_t_1.bar.symbol)
        self.assertEqual(120, bar_1_t_1.bar.open)
        self.assertEqual(130, bar_1_t_1.bar.high)
        self.assertEqual(120, bar_1_t_1.bar.low)
        self.assertEqual(130, bar_1_t_1.bar.close)
        self.assertEqual(2, bar_1_t_1.bar.volume)

    def test_minute_df(self):
        aggregations = Aggregations()
        one_minute_seconds = 60
        symbol_1 = 'SYM1'
        symbol_2 = 'SYM2'

        aggregations.on_trade(Trade(0, symbol_1, 100.0, 1.0))
        aggregations.on_trade(Trade(1, symbol_1, 110.0, 1.0))
        aggregations.on_trade(Trade(one_minute_seconds, symbol_1, 120.0, 1.0))
        aggregations.on_trade(Trade(one_minute_seconds + 1, symbol_1, 130.0, 1.0))
        aggregations.on_trade(Trade(0, symbol_2, 120.0, 1.0))

        df = aggregations.get_minute_df()
        t_zero_epoch = datetime.datetime.utcfromtimestamp(0)
        t_zero_epoch_tz = pytz.utc.localize(t_zero_epoch)
        t_one_minute = datetime.datetime.utcfromtimestamp(60)
        t_one_minute_tz = pytz.utc.localize(t_one_minute)
        self.assertEqual(3, len(df))
        row_0 = df.iloc[0]
        self.assertEqual(t_zero_epoch_tz, row_0.name)
        self.assertEqual('SYM1', row_0.symbol)
        self.assertEqual(100, row_0.open)
        self.assertEqual(110, row_0.high)
        self.assertEqual(100, row_0.low)
        self.assertEqual(110, row_0.close)
        self.assertEqual(2, row_0.volume)

        row_1 = df.iloc[1]
        self.assertEqual(t_one_minute_tz, row_1.name)
        self.assertEqual('SYM1', row_1.symbol)
        self.assertEqual(120, row_1.open)
        self.assertEqual(130, row_1.high)
        self.assertEqual(120, row_1.low)
        self.assertEqual(130, row_1.close)
        self.assertEqual(2, row_1.volume)

        row_2 = df.iloc[2]
        self.assertEqual(t_zero_epoch_tz, row_2.name)
        self.assertEqual('SYM2', row_2.symbol)
        self.assertEqual(120, row_2.open)
        self.assertEqual(120, row_2.high)
        self.assertEqual(120, row_2.low)
        self.assertEqual(120, row_2.close)
        self.assertEqual(1, row_2.volume)

    def test_daily_df(self):
        aggregations = Aggregations()
        one_minute_seconds = 60
        symbol_1 = 'SYM1'
        symbol_2 = 'SYM2'

        aggregations.on_trade(Trade(0, symbol_1, 100.0, 1.0))
        aggregations.on_trade(Trade(1, symbol_1, 110.0, 1.0))
        aggregations.on_trade(Trade(one_minute_seconds, symbol_1, 120.0, 1.0))
        aggregations.on_trade(Trade(one_minute_seconds + 1, symbol_1, 130.0, 1.0))
        aggregations.on_trade(Trade(0, symbol_2, 120.0, 1.0))

        df = aggregations.get_daily_df()
        d_zero_epoch = datetime.date(year=1970, month=1, day=1)

        self.assertEqual(2, len(df))
        row_0 = df.iloc[0]
        self.assertEqual(d_zero_epoch, row_0.name)
        self.assertEqual('SYM1', row_0.symbol)
        self.assertEqual(100, row_0.open)
        self.assertEqual(130, row_0.high)
        self.assertEqual(100, row_0.low)
        self.assertEqual(130, row_0.close)
        self.assertEqual(4, row_0.volume)

        row_1 = df.iloc[1]
        self.assertEqual(d_zero_epoch, row_1.name)
        self.assertEqual('SYM2', row_1.symbol)
        self.assertEqual(120, row_1.open)
        self.assertEqual(120, row_1.high)
        self.assertEqual(120, row_1.low)
        self.assertEqual(120, row_1.close)
        self.assertEqual(1, row_1.volume)

class TestAggregationsRun(unittest.TestCase):
    def test_on_trade(self):
        aggregations_run = AggregationsRun()
        one_minute_seconds = 60
        symbol_1 = 'SYM1'
        symbol_2 = 'SYM2'

        one_day_seconds = 3600 * 24

        aggregations_run.on_trade(Trade(0, symbol_1, 100.0, 1.0))
        aggregations_run.on_trade(Trade(one_minute_seconds, symbol_1, 110.0, 1.0))
        aggregations_run.on_trade(Trade(0, symbol_2, 9.0, 1.0))

        aggregations_run.on_daily_trade_end(base_dir='data_unittest')
        self.assertEqual(0, len(aggregations_run.aggregations.aggregation_per_symbol))
        aggregations_run.on_trade(Trade(one_minute_seconds * 2, symbol_1, 120.0, 1.0))
        self.assertEqual(0, len(aggregations_run.aggregations.aggregation_per_symbol))

        aggregations_run.on_daily_trade_start()
        aggregations_run.on_daily_trade_end(base_dir='data_unittest')
        self.assertEqual(0, len(aggregations_run.aggregations.aggregation_per_symbol))

        aggregations_run.on_daily_trade_start()
        aggregations_run.on_trade(Trade(one_day_seconds, symbol_1, 120.0, 1.0))
        self.assertEqual(1, len(aggregations_run.aggregations.aggregation_per_symbol))
        aggregations_run.on_daily_trade_end(base_dir='data_unittest')
        self.assertEqual(0, len(aggregations_run.aggregations.aggregation_per_symbol))

