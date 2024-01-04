from datetime import datetime
from typing import Literal

import backtrader as bt
import pandas as pd
from pandera import typing as pt

from BasePattern import read_multi_timeframe_base_patterns
from Config import config
from PanderaDFM.BasePattern import MultiTimeframeBasePattern
from PanderaDFM.SignalDf import SignalDf
from Strategy.ExtendedOrder import OrderSide
from Strategy.ExtendedStrategy import ExtendedStrategy
from helper.helper import log_d
from ohlcv import read_base_timeframe_ohlcv


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
        if len(result) > 0:
            pass
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
        size, true_risked_money = self.my_sizer(limit_price=limit_price, sl_price=stop_loss)
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
            'reference_date': [base_pattern_date],
            'reference_timeframe': [base_pattern_timeframe],
            'base_asset_amount': [size],
            'limit_price': [limit_price],
            'stop_loss': [stop_loss],
            'take_profit': [take_profit],
            'trigger_price': [trigger_price],
            'trigger_satisfied': [False],
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
        # cerebro.broker = ExtendedBroker()
        # cerebro.addsizer(MySizer)
        raw_data = read_base_timeframe_ohlcv(date_range_str)
        data = bt.feeds.PandasData(dataname=raw_data, datetime=None, open=0, close=1, high=2, low=3, volume=4,
                                   openinterest=-1)
        cerebro.adddata(data)
        cerebro.broker.set_cash(cash)
        print('Starting Portfolio Value: %.2f' % cerebro.broker.getvalue())
        cerebro.run()
        # todo: test
        print('Final Portfolio Value: %.2f' % cerebro.broker.getvalue())

# class MySizer(bt.Sizer):
#     _initial_cash: float = None
#
#     def initial_cash(self):
#         if self._initial_cash is None:
#             self._initial_cash = self.broker.get_cash()
#         return self._initial_cash
#
#     def _getsizing(self, comminfo, cash, data, isbuy):
#         """
#                 sl_size = abs(Limit price - Stop Loss price)
#                 limit = 1100
#                 sl = 1000
#                 sl_size = 100
#                 initial_cash = 1000
#                 risk_per_order_size = initial_cash * config.risk_per_order_percent = 10
#                 size = risk_size_per_order / sl_size
#
#                 The margin was not implemented by Backtrader but for implementation this could be used:
#                 margin = (size * limit) / risk_size_per_order
#
#                 Parameters:
#                   - cash: The current cash available in the portfolio.
#                   - risk_percent: The percentage of cash to risk per trade (default is 10%).
#
#                 Returns:
#                   - size: The order size to allocate.
#                 """
#         # todoo: test
#         risk_amount = self.initial_cash() * config.order_max_capital_risk_precentage
#         remaining_cash = self.initial_cash() - self.broker.getvalue()
#         remaining_cash = config.initial_cash - sum(self.true_risked_money)
#
#         if remaining_cash >= risk_amount:
#             order: bt.Order = self.strategy.order.executed
#             limit = order.price
#             order_id = order.info['original_order_id']
#             sl_order = self.strategy.stop_orders[order_id]
#             sl = sl_order.price
#             sl_size = abs(sl - limit)
#             risk_per_order_size = config.initial_cash * config.risk_per_order_percent  # 10$
#             size = risk_per_order_size / sl_size
#             true_risked_money = (size * limit) * sl_size
#             assert true_risked_money <= config.risk_per_order_percent
#
#             # # Allocate 1/100 of the initial cash
#             # size = self._initial_cash * config.order_per_order_fixed_base_risk_percentage
#         else:
#             # If remaining cash is less than 10% of initial allocated cash, allocate zero
#             size = 0
#
#         return size
