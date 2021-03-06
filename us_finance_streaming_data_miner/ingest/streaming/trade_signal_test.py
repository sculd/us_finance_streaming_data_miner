import unittest, datetime
import us_finance_streaming_data_miner.util.time as time_util
import numpy as np
import pytz

from us_finance_streaming_data_miner.ingest.streaming.aggregation import Trade
from us_finance_streaming_data_miner.ingest.streaming.trade_signal import TradeSignal
from us_finance_streaming_data_miner.util.current_time import MockCurrentTime


class TestAggregation(unittest.TestCase):
    def test_get_change_df(self):
        symbol = 'DUMMY_SYMBOL'
        signal = TradeSignal(100, symbol)
        signal.on_trade(Trade(0 * 60, symbol, 100.0, 1.0))
        signal.on_trade(Trade(1 * 60, symbol, 110.0, 1.0))
        signal.on_trade(Trade(2 * 60, symbol, 120.0, 1.0))
        signal.on_trade(Trade(3 * 60, symbol, 130.0, 1.0))
        signal.on_trade(Trade(4 * 60, symbol, 140.0, 1.0))

        df_change = signal.get_change_df('close', 2, 1)
        self.assertEqual(1, len(df_change))
        self.assertEqual((140.0-120)/120, df_change.close.values[0])
        self.assertEqual(time_util.truncate_utc_timestamp_to_minute(4 * 60), df_change.iloc[0].name)

        df_change = signal.get_change_df('close', 2, 2)
        self.assertEqual(2, len(df_change))
        self.assertEqual((130.0-110)/110, df_change.close.values[0])
        self.assertEqual((140.0-120)/120, df_change.close.values[1])
        self.assertEqual(time_util.truncate_utc_timestamp_to_minute(3 * 60), df_change.iloc[0].name)
        self.assertEqual(time_util.truncate_utc_timestamp_to_minute(4 * 60), df_change.iloc[1].name)

    def test_get_change_df_overflow(self):
        symbol = 'DUMMY_SYMBOL'
        signal = TradeSignal(100, symbol)
        signal.on_trade(Trade(0 * 60, symbol, 100.0, 1.0))
        signal.on_trade(Trade(1 * 60, symbol, 110.0, 1.0))

        df_change = signal.get_change_df('close', 10, 1)
        self.assertEqual(1, len(df_change))
        self.assertTrue(np.isnan(df_change.close.values[0]))
        self.assertEqual(time_util.truncate_utc_timestamp_to_minute(1 * 60), df_change.iloc[0].name)

    def test_get_value_df(self):
        symbol = 'DUMMY_SYMBOL'
        signal = TradeSignal(100, symbol)
        signal.on_trade(Trade(0 * 60, symbol, 100.0, 1.0))
        # skip minute 1
        signal.on_trade(Trade(2 * 60, symbol, 120.0, 1.0))
        signal.on_trade(Trade(3 * 60, symbol, 130.0, 1.0))
        signal.on_trade(Trade(4 * 60, symbol, 140.0, 1.0))

        df = signal.get_value_df(['close'], 1)
        self.assertEqual(1, len(df))
        self.assertEqual(140.0, df.close.values[0])

        df = signal.get_value_df(['close'], 10)
        self.assertEqual(5, len(df))
        self.assertEqual(100.0, df.close.values[0])
        self.assertEqual(140.0, df.close.values[-1])

        df = signal.get_value_df(['volume'], 1)
        self.assertEqual(1, len(df))
        self.assertEqual(1.0, df.volume.values[0])

        df = signal.get_value_df(['volume'], 10)
        self.assertEqual(5, len(df))
        self.assertEqual(1.0, df.volume.values[0])
        self.assertEqual(1.0, df.volume.values[-1])

    def test_get_quantity_df(self):
        symbol = 'DUMMY_SYMBOL'
        signal = TradeSignal(100, symbol)
        signal.on_trade(Trade(0 * 60, symbol, 100.0, 1.0))
        # skip miniute 1
        signal.on_trade(Trade(2 * 60, symbol, 110.0, 2.0))

        df = signal.get_quantity_df(1)
        self.assertEqual(1, len(df))
        self.assertEqual(110.0, df.close.values[0])
        self.assertEqual(2.0, df.volume.values[0])
        self.assertEqual(220.0, df.quantity.values[0])

        df = signal.get_quantity_df(10)
        self.assertEqual(3, len(df))
        self.assertEqual(100.0, df.quantity.values[0])
        self.assertEqual(0, df.quantity.values[1])
        self.assertEqual(220.0, df.quantity.values[2])

    def test_get_cumulative_quantity_df(self):
        symbol = 'DUMMY_SYMBOL'
        signal = TradeSignal(100, symbol)
        signal.on_trade(Trade(0 * 60, symbol, 100.0, 1.0))
        # skip miniute 1
        signal.on_trade(Trade(2 * 60, symbol, 110.0, 2.0))

        cq = signal.get_cumulative_quantity_df(5)
        self.assertEqual(320.0, cq)


