import datetime, threading, time
import us_finance_streaming_data_miner.util.logging as util_logging
from us_finance_streaming_data_miner.ingest.streaming.aggregation import Aggregation, Aggregations
import us_finance_streaming_data_miner.util.current_time as current_time
from enum import Enum

class TRADE_SIGNAL_MODE(Enum):
    LONG_SIGNAL = 1
    SHORT_SIGNAL = 2
    NO_SIGNAL = 3

class TradeSignal(Aggregation):
    def __init__(self, positionsize, symbol):
        super(TradeSignal, self).__init__(symbol)
        self.in_long_position = False
        self.in_short_position = False
        self.epoch_seconds_long_position_start = 0
        self.epoch_seconds_short_position_start = 0
        self.current_time = current_time.CurrentTime()
        self.last_trade_epoch_seconds = 0
        self.positionsize = positionsize

    def _is_trade_on_new_minute(self):
        epoch_seconds = self.current_time.get_current_epoch_seconds()
        return epoch_seconds // 60 - self.last_trade_epoch_seconds // 60 > 0

    def _update_last_trade_epoch_seconds(self):
        epoch_seconds = self.current_time.get_current_epoch_seconds()
        self.last_trade_epoch_seconds = epoch_seconds

    def on_trade(self, trade):
        super(TradeSignal, self).on_trade(trade)
        self.on_ingest()

    def on_ingest(self):
        if self._is_trade_on_new_minute():
            trading_signal_mode = self.get_is_trading_signal()
            if trading_signal_mode is TRADE_SIGNAL_MODE.LONG_SIGNAL:
                self.enter_long_position()
            elif trading_signal_mode is TRADE_SIGNAL_MODE.SHORT_SIGNAL:
                self.enter_short_position()

        self._update_last_trade_epoch_seconds()

    def get_change_df(self, column_name, change_window_minutes, query_range_minutes):
        '''
        Gets the (cur_val - prev_cal) / prev_cal.

        :param change_window_minutes: the minutes timestamp difference between current and prev
        :param query_range_minutes: e.g. if this is 10 minutes, it gets the change up down to past 10 minutes from now.
        :return:
        '''
        ag = Aggregation(self.symbol)
        ag.bar_with_times = self.bar_with_times[-(query_range_minutes + change_window_minutes):]
        df_ag = ag.get_minute_df(print_log=False).set_index('datetime')[[column_name]]
        df_ag_change = (df_ag.diff(change_window_minutes) / df_ag.shift(change_window_minutes)).iloc[-query_range_minutes:]
        return df_ag_change

    def _get_close_price(self):
        if not len(self.bar_with_times):
            return 0
        bar_with_time = self.bar_with_times[-1]
        return bar_with_time.bar.close

    def _get_change(self):
        '''
        return the change that is used to decide the trading signal.
        :return:
        '''
        df_change = self.get_change_df('close', 10, 1)
        if len(df_change) == 0:
            return False
        change = df_change.close.values[-1]
        return change

    def get_is_trading_signal(self):
        '''
        Gets if the signal is positive for entering in a position.
        :return:
        '''
        return TRADE_SIGNAL_MODE.NO_SIGNAL

    def _on_long_position_enter(self):
        self.in_long_position = True
        self.epoch_seconds_long_position_start = self.current_time.get_current_epoch_seconds()

    def _on_short_position_enter(self):
        self.in_long_position = True
        self.epoch_seconds_short_position_start = self.current_time.get_current_epoch_seconds()

    def on_new_minute(self):
        pass

    def enter_long_position(self):
        self._on_long_position_enter()

    def enter_short_position(self):
        self._on_short_position_enter()

class TradeSignals(Aggregations):
    def __init__(self, dry_run, positionsize, a_current_time = None):
        super(TradeSignals, self).__init__()
        self.last_tick_epoch_second = 0
        self.current_time = a_current_time if a_current_time else current_time.CurrentTime()
        self.tick_minute_sleep_duration_seconds = 10
        self.positionsize = positionsize
        threading.Thread(target=self._tick_minute).start()

    def _is_tick_new_minute(self):
        epoch_seconds = self.current_time.get_current_epoch_seconds()
        return epoch_seconds // 60 - self.last_tick_epoch_second // 60 > 0

    def _tick_minute(self):
        while True:
            if self._is_tick_new_minute():
                self.on_new_minute()
                self.last_tick_epoch_second = self.current_time.get_current_epoch_seconds()
            time.sleep(self.tick_minute_sleep_duration_seconds)

    def on_bar_with_time(self, bar_with_time):
        if bar_with_time.bar.symbol not in self.aggregation_per_symbol:
            self.aggregation_per_symbol[bar_with_time.bar.symbol] = TradeSignal(self.positionsize, bar_with_time.bar.symbol)
        super(TradeSignals, self).on_bar_with_time(bar_with_time)

    def on_trade(self, trade):
        if trade.symbol not in self.aggregation_per_symbol:
            self.aggregation_per_symbol[trade.symbol] = TradeSignal(self.positionsize, trade.symbol)
        super(TradeSignals, self).on_trade(trade)

    def on_new_minute(self):
        util_logging.debug("TradeSignals.on_new_minute")
        for _, aggregation in self.aggregation_per_symbol.items():
            aggregation.on_new_minute()
