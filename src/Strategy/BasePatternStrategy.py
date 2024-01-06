from datetime import datetime
from time import strptime
from typing import Literal

import backtrader as bt
import pandas as pd
from pandera import typing as pt

from BasePattern import read_multi_timeframe_base_patterns
from Config import config
from PanderaDFM.BasePattern import MultiTimeframeBasePattern, BasePattern
from PanderaDFM.SignalDf import SignalDf
from Strategy.order_helper import OrderSide, BracketOrderType
from Strategy.ExtendedStrategy import ExtendedStrategy
from helper.helper import log_d, log_e, measure_time
from ohlcv import read_base_timeframe_ohlcv


class BasePatternStrategy(ExtendedStrategy):
    base_patterns: pt.DataFrame[MultiTimeframeBasePattern]

    def add_base_patterns(self):
        self.base_patterns = read_multi_timeframe_base_patterns(self.date_range_str)

    @measure_time
    def __init__(self):
        self.add_base_patterns()
        super().__init__()

    def notify_order(self, order: bt.Order):
        if order.status == bt.Order.Completed:
            if order.info['custom_type'] == BracketOrderType.Stop.value:
                # todo: if the order were stopped, repeat the signal
                # todo: assure closing of the original and profit orders
                # a bracket order stopp-loss executed.
                index = order.info['signal_index']
                signal = self.signal_df.loc[index]
                log_d(f'repeating Signal  according to Stop-Loss on {SignalDf.to_str(index, signal)} @ {self.candle()}')
                repeat_signal = signal.copy()
                repeat_signal['original_index'] = index
                repeat_signal['order_is_active'] = False
                repeat_signal['original_order_id'] = pd.NA
                repeat_signal['end'] = pd.NA
                self.signal_df.loc[self.candle().date] = repeat_signal
                log_d(f"repeated Signal:{SignalDf.to_str(self.candle().date, repeat_signal)}@{self.candle().date} ")
            if order.info['custom_type'] == BracketOrderType.Profit.value:
                # todo: if the order took profit, end signal and end band_pattern
                # todo: assure closing of the original and stop orders
                log_d(f'Took profit on Signal:{SignalDf.to_str(index, signal)}@{self.candle().date}')

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
        # size, true_risked_money = self.my_sizer(limit_price=limit_price, sl_price=stop_loss)
        # todo: reference_date and reference_timeframe never been used.
        # todo: use .loc to generate and assign signal in one step.
        if pd.notna(base_pattern['end']):
            effective_end = base_pattern['end']
        else:
            effective_end = base_pattern['ttl']
        try:
            # todo: optimize and simplify by removing signals and putting orders instead.
            new_signal = SignalDf.new({
                'date': [self.candle().date],
                'original_index': [self.candle().date],
                'end': [effective_end],
                'side': [side],
                'reference_date': [base_pattern_date],
                'reference_timeframe': [base_pattern_timeframe],
                # 'base_asset_amount': [size],
                'limit_price': [limit_price],
                'stop_loss': [stop_loss],
                'take_profit': [take_profit],
                'trigger_price': [trigger_price],
                'trigger_satisfied': [False],
                'order_is_active': [False],
            })
        except Exception as e:
            raise e
        self.signal_df = SignalDf.concat(self.signal_df, new_signal)
        log_d(f"added Signal {SignalDf.to_str(new_signal.index[0], new_signal.iloc[0])} @ {self.candle().date}")
        return self.signal_df

    def candle_overlaps_base(self, base_pattern: pt.Series[BasePattern]):
        if self.candle().date == strptime("2024-01-03 02:36:00+00:00", "%Y-%m-%d %H:%M:%S%z"):
            pass
        if self.movement_intersect(base_pattern['internal_low'], base_pattern['internal_high'])>0:
            return True
        else:
            return False

    # @measure_time
    def extract_signals(self) -> None:
        if self.candle().date == strptime("2024-01-03 02:36:00+00:00", "%Y-%m-%d %H:%M:%S%z"):
            pass
        upper_overlapping_base_patterns = self.overlapping_base_patterns(
            self.no_signal_active_base_patterns(band='upper', base_patterns=self.base_patterns))
        base_pattern: pt.Series[MultiTimeframeBasePattern]
        for (timeframe, start), base_pattern in upper_overlapping_base_patterns.iterrows():
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
        print('Final Portfolio Value: %.2f' % cerebro.broker.getvalue())  # todo: test
    def next(self):
        # todo: if the signal end changed we have to update signal orders.
        # todo: check It only happens when order takes profit. In this case both of sell and
        super().next()