import datetime
from enum import Enum

class POSITION_MODE(Enum):
    NO_POSITION = 1
    SHORT_SEEKING_ENTRY = 2
    SHORT_ENTERED = 3
    LONG_SEEKING_ENTRY = 4
    LONG_ENTERED = 5

class TRADING_ACTION(Enum):
    NO_ACTION = 1
    ENTER_LONG = 2
    EXIT_LONG = 3
    ENTER_SHORT = 4
    EXIT_SHORT = 5

class TradingLogic:
    def __init__(self, symbol):
        self.symbol = symbol
        self.position_mode = POSITION_MODE.NO_POSITION

        self.long_enter_price = 0
        self.epoch_seconds_long_position_start = 0

        self.short_enter_price = 0
        self.epoch_seconds_short_position_start = 0

    def _get_datetime_str(self, epoch_seconds):
        return str(datetime.datetime.utcfromtimestamp(epoch_seconds))

    def _update_position_mode_on_new_minute(self, close_price, trading_signal_mode, current_epoch_seconds):
        self.position_mode = POSITION_MODE.NO_POSITION

    def update_position_mode_get_trading_action(self, close_price, trading_signal_mode, current_epoch_seconds):
        prev_position_mode = self.position_mode
        self._update_position_mode_on_new_minute(close_price, trading_signal_mode, current_epoch_seconds)
        new__position_mode = self.position_mode

        if prev_position_mode == new__position_mode:
            return TRADING_ACTION.NO_ACTION

        if new__position_mode is POSITION_MODE.SHORT_ENTERED:
            return TRADING_ACTION.ENTER_SHORT

        if new__position_mode is POSITION_MODE.LONG_ENTERED:
            return TRADING_ACTION.ENTER_LONG

        if new__position_mode is POSITION_MODE.NO_POSITION:
            if prev_position_mode is POSITION_MODE.SHORT_ENTERED:
                return TRADING_ACTION.EXIT_SHORT
            if prev_position_mode is POSITION_MODE.LONG_ENTERED:
                return TRADING_ACTION.EXIT_LONG

        return TRADING_ACTION.NO_ACTION

    def on_trading_action_done(self, trading_action, target_price, epoch_seconds):
        if trading_action is TRADING_ACTION.ENTER_SHORT:
            self.short_enter_price = target_price
            self.epoch_seconds_short_position_start = epoch_seconds

        elif trading_action is TRADING_ACTION.ENTER_LONG:
            self.long_enter_price = target_price
            self.epoch_seconds_long_position_start = epoch_seconds
