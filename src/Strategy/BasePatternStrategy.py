from datetime import datetime
from enum import Enum
from typing import Literal, Dict

import backtrader as bt
import numpy as np
import pandas as pd
import pytz
from pandera import typing as pt

from BasePattern import read_multi_timeframe_base_patterns
from Config import config
from PanderaDFM.BasePattern import MultiTimeframeBasePattern
from PanderaDFM.BaseTickStructure import BaseTickStructure
from PanderaDFM.SignalDf import SignalDf, SignalDFM
from Strategy.ExtendedOrder import ExtendedOrder
from helper.helper import log, log_d
from ohlcv import read_base_timeframe_ohlcv


class OrderSide(Enum):
    Buy = 'buy'
    Sell = 'sell'


class MySizer(bt.Sizer):
    def _getsizing(self, comminfo, cash, data, isbuy):
        """
                Custom sizer function to allocate order sizes.

                Parameters:
                  - cash: The current cash available in the portfolio.
                  - risk_percent: The percentage of cash to risk per trade (default is 10%).

                Returns:
                  - size: The order size to allocate.
                """
        # todo: test
        assert self.initial_cash is not None and self.initial_cash > 0
        risk_amount = self.initial_cash * config.order_max_capital_risk_precentage
        remaining_cash = self.initial_cash - self.broker.getvalue()

        if remaining_cash >= risk_amount:
            # Allocate 1/100 of the initial cash
            size = self.initial_cash * config.order_per_order_fixed_base_risk_percentage
        else:
            # If remaining cash is less than 10% of initial allocated cash, allocate zero
            size = 0

        return size


class ExtendedStrategy(bt.Strategy):
    signal_df: pt.DataFrame[SignalDFM] = SignalDf.new()
    main_orders: Dict[np.float64, bt.Order] = {}
    stop_orders: Dict[np.float64, bt.Order] = {}
    profit_orders: Dict[np.float64, bt.Order] = {}
    archived_orders: Dict[np.float64, bt.Order] = {}
    date_range_str: str = None
    initial_cash: float = None

    def candle(self, backward_index: int = 0) -> BaseTickStructure:
        return BaseTickStructure(
            date=bt.num2date(self.datas[0].datetime[backward_index]).replace(tzinfo=pytz.UTC),
            close=self.datas[0].close[backward_index],
            open=self.datas[0].open[backward_index],
            high=self.datas[0].high[backward_index],
            low=self.datas[0].low[backward_index],
            volume=self.datas[0].volume[backward_index],
        )

    # def debug_datas_structure(self):
    #     for i, data in enumerate(self.datas[:min(5, len(self.datas))]):
    #         print(f"Data Feed {i + 1}:", type(data))
    #         print("Attributes:")
    #         for attr in dir(data):
    #             if not callable(getattr(data, attr)) and not attr.startswith("__"):
    #                 print(f"  {attr}: {getattr(data, attr)}")
    #         try:
    #             self.candle()
    #             log("Cast-able to predefined DatasStructure.")
    #         except TypeError:
    #             log("Failed to Cast to predefined DatasStructure:")
    #         except Exception as e:
    #             raise e

    def set_date_range(self, date_range_str: str = None):
        if date_range_str is None:
            date_range_str = config.processing_date_range
        self.date_range_str = date_range_str
        # self.start_datime, self.end = date_range(date_range_str)

    def __init__(self):
        self.signal_df = SignalDf.new()
        self.set_date_range()

    def notify_order(self, order: bt.Order):
        # todo: test
        assert order.__str__() in self.signal_df['order_id']
        assert all(self.signal_df.loc[self.signal_df['order_id'] == order.__str__(), 'order_is_active']
                   == True)
        assert order.size > 0
        assert order.size > 1 / 43000, "Order size less than 1 dollar"
        if order.status in [order.Submitted, order.Accepted]:
            # Order has been submitted or accepted
            log_d(f"Order:{order} submission confirmed")
        elif order.status in [order.Completed]:
            # Order has been completed (executed)
            self.signal_df.loc[self.signal_df['order_id'] == order.__str__(), 'order_is_active'] = False
            self.signal_df.loc[self.signal_df['order_id'] == order.__str__(), 'order_id'] = pd.NA
            log_d(f"Order:{order} Completed")
        elif order.status in [order.Partial]:
            log_d(f"Order:{order} Partially executed")
        elif order.status in [order.Expired]:
            raise Exception(f"Order:{order} Expired")
        elif order.status in [order.Canceled]:
            raise Exception(f"Order:{order} Canceled")
        elif order.status in [order.Rejected]:
            raise Exception(f"Order:{order} Rejected")
        else:
            raise Exception(f"Order:{order} Unexpected status {order.status}")

    def ordered_signals(self, signal_df: SignalDf = None) -> SignalDf:
        if signal_df is None:
            signal_df = self.signal_df
        return signal_df[signal_df['order_is_active'].notna() & signal_df['order_is_active']]

    def next(self):
        if len(self.main_orders) > 0:
            self.verify_triple_oder_status()
        # self.update_bases() the base patterns are prepared before!
        self.extract_signals()
        # self.update_orders()
        self.update_ordered_signals()
        self.execute_active_signals()

    def active_signals(self) -> SignalDf:
        if 'end' not in self.signal_df.columns:
            # self.signal_df['end'] = pd.Series(dtype='datetime64[ns, UTC]')
            pass
        result = self.signal_df[
            (self.signal_df.index.get_level_values(level='date') <= self.candle().date) &
            (
                    ('end' not in self.signal_df.columns) |
                    (self.signal_df['end'].isna()) |
                    (self.signal_df['end'] > self.candle().date)) &
            (self.signal_df['order_is_active'].isna() | ~self.signal_df['order_is_active'])
            ]
        return result

    @classmethod
    def add_order_info(cls, order: bt.Order, signal, signal_start, order_type: Literal['main', 'stop', 'profit']):
        order.addinfo(signal=signal)
        order.addinfo(signal_start=signal_start)
        order.addinfo(custom_type=order_type)

    def update_ordered_signals(self):
        ordered_signals = self.ordered_signals()
        for index, signal in ordered_signals.iterrows():
            # todo: test
            if SignalDf.stopped(signal, self.candle()):
                log(f'Signal repeated according to Stop-Loss on {SignalDf.to_str(index, signal)} @ {self.candle()}')
                repeat_signal = signal.copy()
                repeat_signal['original_index'] = index
                repeat_signal['order_is_active'] = False
                repeat_signal['order_id'] = pd.NA
                repeat_signal['led_to_order_at'] = pd.NA
                self.signal_df.loc[self.candle().date] = repeat_signal
                log_d(f"repeated Signal:{repeat_signal}@{self.candle()} ")
            elif SignalDf.took_profit(signal, self.candle()):
                log_d(f'Took profit on Signal:{SignalDf.repr(index, signal)}@{self.candle()}')
            elif SignalDf.expired(signal, self.candle()):
                log(f'Signal:{SignalDf.repr(index, signal)}@{self.candle()} Expired')

    def execute_active_signals(self):
        assert 'order_id' in self.signal_df.columns
        active_signals = self.active_signals()
        for start, signal in active_signals.iterrows():
            # todo: test
            bracket_executor, order_executor = self.executors(signal)
            execution_type = SignalDf.execution_type(signal)
            if pd.notna(signal['take_profit']):
                main_order: bt.Order
                # todo: check_trigger does not inherit from ExtendedOrder and does not accept trigger_price=signal['trigger_price']
                main_order, stop_order, profit_order = bracket_executor(size=signal['base_asset_amount'],
                                                                        exectype=execution_type,
                                                                        limitprice=signal['take_profit'],
                                                                        price=signal['limit_price'],
                                                                        stopprice=signal['stop_loss'],
                                                                        trigger_price=signal['trigger_price'])
                order_id = np.float64(datetime.now(tz=pytz.UTC).timestamp())
                self.add_order_info(main_order, signal, start, 'main')
                self.add_order_info(stop_order, signal, start, 'stop')
                self.add_order_info(profit_order, signal, start, 'profit')
                self.main_orders[order_id] = main_order
                self.stop_orders[order_id] = stop_order
                self.profit_orders[order_id] = profit_order
                # order_executor(
                #     size=base_asset_amount, exectype=execution_type, price=take_profit, parent=bracket_executor(
                #         limitprice=take_profit, stopprice=stop_loss
                #     )
                # )
                self.signal_df.loc[start, 'order_id'] = order_id
                log_d(f"Signal:{signal} ordered "
                      f"M:{main_order.__str__()} S:{stop_order.__str__()} P:{profit_order.__str__()}")
            else:
                raise Exception('Expected all signals have take_profit')
                # order_id = order_executor(size=signal['base_asset_amount'], exectype=execution_type,
                #                                price=signal['stop_loss'],
                #                                limit_price=signal['limit_price'],
                #                                trigger_price=signal['trigger_price'])
                # self.signal_df.loc[start, 'order_id'] = order_id

    def verify_triple_oder_status(self):
        '''
        check:
         - length of self.main_orders and alef.stop_orders and self.profit_orders are equal
         - if the self.main_orders.status is open the same index in both other lists
         have the same status
         - if the self.main_orders.status in closed (by any way) the same index in both other lists are closed
         (canceled) also
        :return:
        '''
        assert len(self.main_orders) == len(self.stop_orders)
        assert len(self.main_orders) == len(self.profit_orders)
        for index, order in self.main_orders:
            if ExtendedOrder.is_open(order):
                assert ExtendedOrder.is_open(self.stop_orders[index])
                assert ExtendedOrder.is_open(self.profit_orders[index])

    def executors(self, signal):
        if signal['side'] == 'buy':
            order_executor = self.buy
            bracket_executor = self.sell_bracket
        elif signal['side'] == 'sell':
            # todo: test
            order_executor = self.sell
            bracket_executor = self.buy_bracket
            self.check_trigger()
        else:
            raise Exception('Unknown side %s' % signal['side'])
        return bracket_executor, order_executor


class BasePatternStrategy(ExtendedStrategy):
    base_patterns: pt.DataFrame[MultiTimeframeBasePattern]

    def add_base_patterns(self):
        self.base_patterns = read_multi_timeframe_base_patterns(self.date_range_str)

    def __init__(self):
        self.add_base_patterns()
        super().__init__()

    def overlapping_base_patterns(self, base_patterns: pt.DataFrame[MultiTimeframeBasePattern] = None) \
            -> pt.DataFrame[MultiTimeframeBasePattern]:
        if base_patterns is None:
            base_patterns = self.base_patterns
        if len(base_patterns[['end', 'ttl']].min(axis='columns', skipna=True)) != len(base_patterns):
            pass
        effective_end = base_patterns[['end', 'ttl']].min(axis='columns', skipna=True)
        try:
            hit_base_patterns = base_patterns[
                (base_patterns.index.get_level_values(level='date') < self.candle().date) &
                (effective_end > self.candle().date) &
                (base_patterns['internal_high'] > self.candle().low) &
                (base_patterns['internal_low'] < self.candle().high)
                ]
        except Exception as e:
            raise e
        return hit_base_patterns

    def no_signal_active_base_patterns(self, band: Literal['upper', 'below'],
                                       base_patterns: pt.DataFrame[MultiTimeframeBasePattern] = None) -> \
            pt.DataFrame[MultiTimeframeBasePattern]:
        if base_patterns is None:
            base_patterns = self.base_patterns
        if f'{band}_band_signal_generated' not in base_patterns.columns:
            base_patterns[f'{band}_band_signal_generated'] = pd.NA

        result = base_patterns[
            (base_patterns[f'{band}_band_activated'].notna() & (
                    base_patterns[f'{band}_band_activated'] <= self.candle().date)) &
            (base_patterns[f'{band}_band_signal_generated'].isna())
            ]
        return result

    def add_signal(self, base_pattern_timeframe: str, base_pattern_date: datetime,
                   base_pattern: pt.Series[MultiTimeframeBasePattern],
                   band: Literal['upper', 'below']) -> pt.DataFrame[SignalDf.schema_data_frame_model]:
        base_length = base_pattern['internal_high'] - base_pattern['internal_low']
        if band == 'upper':
            side = OrderSide.Buy.value
            limit_price = (base_pattern['internal_high'] +
                           base_pattern['ATR'] * config.base_pattern_order_limit_price_margin_percentage)
            stop_loss = base_pattern['internal_low']
            take_profit = base_pattern['internal_high'] + base_length * config.base_pattern_risk_reward_rate
            trigger_price = base_pattern['internal_high']
        else:  # band == 'below':
            side = OrderSide.Sell.value
            limit_price = (base_pattern['internal_low'] -
                           base_pattern['ATR'] * config.base_pattern_order_limit_price_margin_percentage)
            stop_loss = base_pattern['internal_high']
            take_profit = base_pattern['internal_low'] - base_length * config.base_pattern_risk_reward_rate
            trigger_price = base_pattern['internal_low']
        # todo: reference_multi_date and reference_multi_timeframe never been used.
        # todo: use .loc to generate and assign signal in one step.
        if pd.notna(base_pattern['end']):
            effective_end = base_pattern['end']
        else:
            effective_end = base_pattern['ttl']
        new_signal = SignalDf.new({
            'date': [self.candle().date],
            'end': [effective_end],
            'side': [side],
            'reference_multi_date': [base_pattern_date],
            'reference_multi_timeframe': [base_pattern_timeframe],
            # base_asset_amount=self.sizer,
            'limit_price': [limit_price],
            'stop_loss': [stop_loss],
            'take_profit': [take_profit],
            'trigger_price': [trigger_price],
            'order_is_active': [False],
        })
        self.signal_df = SignalDf.concat(self.signal_df, new_signal)
        log_d(f"added Signal {SignalDf.to_str(new_signal)} @ {self.candle().date}")
        return self.signal_df

    def candle_overlaps_base(self, base_pattern):
        return (
                self.candle().high > base_pattern['internal_low'] and
                self.candle().low < base_pattern['internal_high'])

    def extract_signals(self) -> None:
        upper_band_active_overlapping_base_patterns = self.overlapping_base_patterns(
            self.no_signal_active_base_patterns(band='upper', base_patterns=self.base_patterns))
        base_pattern: pt.Series[MultiTimeframeBasePattern]
        for (timeframe, start), base_pattern in upper_band_active_overlapping_base_patterns.iterrows():
            if self.candle_overlaps_base(base_pattern):
                self.add_signal(timeframe, start, base_pattern, band='upper')
                self.base_patterns.loc[(timeframe, start), 'upper_band_signal_generated'] = self.candle().date
        below_overlapping_base_patterns = self.overlapping_base_patterns(
            self.no_signal_active_base_patterns(band='below', base_patterns=self.base_patterns))
        for (timeframe, start), base_pattern in below_overlapping_base_patterns.iterrows():
            if self.candle_overlaps_base(base_pattern):
                self.add_signal(timeframe, start, base_pattern, band='below')
                self.base_patterns.loc[(timeframe, start), 'below_band_signal_generated'] = self.candle().date

    @staticmethod
    def test_strategy(cash: float, date_range_str: str = None):
        if date_range_str is None:
            date_range_str = config.processing_date_range
        cerebro = bt.Cerebro()
        cerebro.addstrategy(BasePatternStrategy)
        cerebro.addsizer(MySizer)
        raw_data = read_base_timeframe_ohlcv(date_range_str)
        data = bt.feeds.PandasData(dataname=raw_data, datetime=None, open=0, close=1, high=2, low=3, volume=4,
                                   openinterest=-1)
        cerebro.adddata(data)
        cerebro.broker.set_cash(cash)
        print('Starting Portfolio Value: %.2f' % cerebro.broker.getvalue())
        cerebro.run()
        # todo: test
        print('Final Portfolio Value: %.2f' % cerebro.broker.getvalue())


def order_name(cls, order: bt.Order):
    # todo: test
    # TRX<13USDT@0.02
    name = (f"Order"
            f"{('<' if order.isbuy() else '>')}"
            f"{order.size}")
    if order.pricelimit is not None:
        name += f"@{order.pricelimit}"
