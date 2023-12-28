from datetime import datetime

import backtrader as bt
import pandas as pd
from pandera import typing as pt

from BasePattern import read_multi_timeframe_base_patterns
from Config import config
from Model.BasePattern import MultiTimeframeBasePattern
from Model.Signal import Signal
from helper import date_range
from ohlcv import read_base_timeframe_ohlcv


class ExtendedOrder(bt.Order):
    trigger_satisfied: bool = False

    def __init__(self, parent, limit_price, stop_loss, take_profit, trigger_price, *args, **kwargs):
        self.trigger_price = trigger_price
        self.limit_price = limit_price
        self.stop_loss = stop_loss
        self.take_profit = take_profit
        super().__init__(parent, *args, **kwargs)

    def check_trigger(self, price):
        if self.isbuy():
            return price >= self.trigger_price
        elif self.issell():
            return price <= self.trigger_price
        return False

    def __init__(self, parent, trigger_price, *args, **kwargs):
        self.trigger_price = trigger_price
        super().__init__(parent, *args, **kwargs)

    def execute(self):
        # Check trigger condition before executing
        if self.check_trigger():
            # Perform the actual order execution
            return super().execute()
        else:
            # Hold the order if trigger condition is not satisfied
            # self.parent.log("Trigger condition not satisfied. Order on hold.")
            return None

    def check_trigger(self):
        # Implement your trigger condition logic here
        # Return True if the condition is satisfied, False otherwise
        if self.isbuy():
            self.trigger_satisfied |= (self.parent.data.close[0] >= self.trigger_price)
        else:  # self.issell()
            self.trigger_satisfied |= self.parent.data.close[0] <= self.trigger_price
        return self.trigger_satisfied


class BasePatternStrategy(bt.Strategy):
    signal_df: Signal
    date_range_str: str
    start: datetime
    end: datetime
    base_patterns: pt.DataFrame[MultiTimeframeBasePattern]

    def log(self, txt, dt=None):
        ''' Logging function for this strategy'''
        dt = dt or self.datas[0].datetime.date(0)
        print('%s, %s' % (dt.isoformat(), txt))

    # def add_signal_source(self, signal_source: Signal):
    #     self.signal_df = signal_source
    #     Signal.cast_and_validate(signal_source)
    def add_base_patterns(self):
        self.base_patterns = read_multi_timeframe_base_patterns(
            self.date_range_str)

    def set_date_range(self, date_range_str):

    def __init__(self):
        self.set_date_range()
        self.add_base_patterns()

    def next(self):
        """
        I have a df with schema of Signal, I want to check if current testing time is after a signal and the time is
        before signal end or the signal end is np.INF or NA, and the order related to the signal has not been placed
        yet, put the order according to the signal.
        :return:
        """
        self.log('Close, %.2f' % self.data[0])
        self.update_bases()
        self.extract_signals()
        self.execute_active_signals()

    def update_bases(self):
        not_implemented()

    def ovelapping_patterns(self) -> pt.DataFrame[MultiTimeframeBasePattern]:
        self.base_patterns['effective_end'] = self.base_patterns[['end', 'ttl']].min(axis='columns', skipna=True)
        hit_base_patterns = self.base_patterns[
            (self.base_patterns['start'] < self.date()) &
            (self.base_patterns['effective_end'] > self.date()) &
            (self.base_patterns['internal_high'] > self.low()) &
            (self.base_patterns['internal_low'] < self.high())
            ]
        return hit_base_patterns

    def not_executed_upper_band_active_base_parrents(self, order):
        result = self.base_patterns[
            (self.base_patterns['upper_band_activated'].isna()) &
            (self.base_patterns['upper_band_signal_generated'].isna())
            ]
        return result

    def no_signal_below_band_active_base_parrents(self, order):
        result = self.base_patterns[
            (self.base_patterns['below_band_activated'].isna()) &
            (self.base_patterns['below_band_signal_generated'].isna())
            ]
        return result

    def extract_signals(self):
        overlapping_patterns = self.ovelapping_patterns()
        not_executed_upper_band_active_base_parrents = self.not_executed_upper_band_active_base_parrents()
        for star, base_pattern in self.not_executed_upper_band_active_base_parrents().iterrows():
            not_implemented()
            pass
        for star, base_pattern in self.no_signal_below_band_active_base_parrents().iterrows():
            not_implemented()
            pass

    def execute_active_signals(self):
        _datetime = self.datas[0].datetime.datetime(0)
        if 'main_order_id' not in self.signal_df.columns:
            self.signal_df['main_order_id'] = pd.NA
        active_signals = self.signal_df[
            (self.signal_df['date'] <= _datetime) &
            ((self.signal_df['end'].isna()) | (self.signal_df['end'] > _datetime)) &
            (self.signal_df['main_order_id'].isna())
            ]
        for start, signal in active_signals.iterrows():
            # Placeholder for actual order placement logic
            # Replace this with your order placement code
            bracket_executor, order_executor = self.executors(signal)
            # Placeholder for setting stop loss and take profit
            execution_type = self.execution_type()
            if pd.notna(signal['take_profit']):
                main_order_id, _, _ = bracket_executor(size=signal['base_asset_amount'], exectype=execution_type,
                                                       limitprice=signal['take_profit'],
                                                       price=signal['limit_price'], stopprice=signal['stop_loss'],
                                                       trigger_price=signal['trigger_price'])
                # order_executor(
                #     size=base_asset_amount, exectype=execution_type, price=take_profit, parent=bracket_executor(
                #         limitprice=take_profit, stopprice=stop_loss
                #     )
                # )
                self.signal_df.loc[start, 'main_order_id'] = main_order_id
            else:
                main_order_id = order_executor(size=signal['base_asset_amount'], exectype=execution_type,
                                               price=signal['stop_loss'],
                                               limit_price=signal['limit_price'], trigger_price=signal['trigger_price'])
                self.signal_df.loc[start, 'main_order_id'] = main_order_id

    def executors(self, signal):
        if signal['side'] == 'buy':
            order_executor = self.buy
            bracket_executor = self.sell_bracket
        elif signal['side'] == 'sell':
            order_executor = self.sell
            bracket_executor = self.buy_bracket
        else:
            raise Exception('Unknown side %s' % signal['side'])
        return bracket_executor, order_executor

    @staticmethod
    def execution_type(signal):
        if pd.notna(signal['stop_loss']) and pd.notna(signal['limit_price']):
            execution_type = ExtendedOrder.StopLimit
        elif pd.notna(signal['stop_loss']):
            execution_type = ExtendedOrder.Stop
        elif pd.notna(signal['limit_price']):
            execution_type = ExtendedOrder.Limit
        else:
            execution_type = ExtendedOrder.Market
        return execution_type

    def set_date_range(self, date_range_str: str = None):
        if date_range_str is None:
            self.date_range_str = config.processing_date_range
        self.start, self.end = date_range(date_range_str)


def test_strategy(date_range_str: str = None):
    if date_range_str is None:
        date_range_str = config.processing_date_range
    cerebro = bt.Cerebro()
    cerebro.addstrategy(BasePatternStrategy)
    raw_data = read_base_timeframe_ohlcv(date_range_str)
    data = bt.feeds.PandasData(dataname=raw_data, datetime=None, open=0, close=1, high=2, low=3, volume=4,
                               openinterest=-1)
    cerebro.adddata(data)
    cerebro.broker.set_cash(100.0)
    print('Starting Portfolio Value: %.2f' % cerebro.broker.getvalue())
    cerebro.run()
    print('Final Portfolio Value: %.2f' % cerebro.broker.getvalue())
