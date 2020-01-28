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
