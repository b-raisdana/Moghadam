from datetime import datetime
from enum import Enum
from typing import List, Literal

import backtrader as bt
import pandas as pd
import pytz
from pandera import typing as pt

from BasePattern import read_multi_timeframe_base_patterns
from Config import config
from Model.BasePattern import MultiTimeframeBasePattern
from Model.BaseTickStructure import BaseTickStructure
from Model.ExtendedOrder import ExtendedOrder
from Model.SignalDf import SignalDf
from helper import date_range, log, LogSeverity
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


class BasePatternStrategy(bt.Strategy):
    signal_df: SignalDf
    date_range_str: str
    # start_datime: datetime
    # end: datetime
    base_patterns: pt.DataFrame[MultiTimeframeBasePattern]
    datas: List[BaseTickStructure]
    initial_cash: float = None

    def get_datas(self, backward_index: int = 0):
        return BaseTickStructure(
            date=bt.num2date(self.datas[0].datetime[backward_index]).replace(tzinfo=pytz.UTC),
            close=self.datas[0].close[backward_index],
            open=self.datas[0].open[backward_index],
            high=self.datas[0].high[backward_index],
            low=self.datas[0].low[backward_index],
            volume=self.datas[0].volume[backward_index],
        )

    def debug_datas_structure(self):
        # todo: test
        for i, data in enumerate(self.datas[:min(5, len(self.datas))]):
            print(f"Data Feed {i + 1}:", type(data))
            print("Attributes:")
            for attr in dir(data):
                if not callable(getattr(data, attr)) and not attr.startswith("__"):
                    print(f"  {attr}: {getattr(data, attr)}")
            try:
                a = BaseTickStructure(self.get_datas())
                log("Cast-able to predefined DatasStructure.", severity=LogSeverity.DEBUG, stack_trace=False)
            except TypeError:
                log("Failed to Cast to predefined DatasStructure:", severity=LogSeverity.ERROR, stack_trace=False)
            except Exception as e:
                raise e

    def add_base_patterns(self):
        # todo: test
        self.base_patterns = read_multi_timeframe_base_patterns(
            self.date_range_str)

    def set_date_range(self, date_range_str: str = None):
        # todo: test
        if date_range_str is None:
            date_range_str = config.processing_date_range
        self.date_range_str = date_range_str
        # self.start_datime, self.end = date_range(date_range_str)

    def __init__(self):
        # todo: test
        self.signal_df = SignalDf.new()
        self.set_date_range()
        self.add_base_patterns()

    def set_sizer(self):
        # todo: test
        # only runs once on first data
        if self.initial_cash is None:
            self.initial_cash = self.broker.get_cash()
            if self.initial_cash is None or not self.initial_cash > 0:
                raise Exception(f"Cash expected to be positive but is {self.initial_cash}")
            super(MySizer)

    def next(self):
        self.set_sizer()
        # self.update_bases() the base patterns are prepared before!
        self.extract_signals()
        self.update_executed_orders()
        # todo: test
        self.execute_active_signals()

    def overlapping_base_patterns(self, base_patterns: pt.DataFrame[MultiTimeframeBasePattern] = None) \
            -> pt.DataFrame[MultiTimeframeBasePattern]:
        if base_patterns is None:
            base_patterns = self.base_patterns
        base_patterns['effective_end'] = base_patterns[['end', 'ttl']].min(axis='columns', skipna=True)
        hit_base_patterns = base_patterns[
            (base_patterns.index.get_level_values(level='date') < self.get_datas().date) &
            (base_patterns['effective_end'] > self.get_datas().date) &
            (base_patterns['internal_high'] > self.get_datas().low) &
            (base_patterns['internal_low'] < self.get_datas().high)
            ]
        return hit_base_patterns

    def no_signal_active_base_patterns(self, direction: Literal['upper', 'below'],
                                       base_patterns: pt.DataFrame[MultiTimeframeBasePattern] = None) -> \
            pt.DataFrame[MultiTimeframeBasePattern]:
        # todo: test
        if base_patterns is None:
            base_patterns = self.base_patterns
        if f'{direction}_band_signal_generated' not in base_patterns.columns:
            base_patterns[f'{direction}_band_signal_generated'] = pd.NA
        else:
            pass
        result = base_patterns[
            (base_patterns[f'{direction}_band_activated'].notna() & (
                    base_patterns[f'{direction}_band_activated'] > self.get_datas().date)) &
            (base_patterns[f'{direction}_band_signal_generated'].isna())
            ]
        return result

    def add_signal(self, base_pattern_index, base_pattern: pt.Series[MultiTimeframeBasePattern],
                   band: Literal['upper', 'below']) -> SignalDf:
        """
        date: pt.Index[Annotated[pd.DatetimeTZDtype, "ns", "UTC"]]
        reference_multi_index: pt.Series[Union[Tuple[str, Annotated[pd.DatetimeTZDtype, "ns", "UTC"]]]]
        end: pt.Index[Annotated[pd.DatetimeTZDtype, "ns", "UTC"]] = pandera.Field(nullable=True)
        side: pt.Series[str]  # sell or buy
        base_asset_amount: pt.Series[float]
        limit_price: pt.Series[float]  # = pandera.Field(nullable=True)
        stop_loss: pt.Series[float] = pandera.Field(nullable=True)
        take_profit: pt.Series[float]  # = pandera.Field(nullable=True)
        trigger_price: pt.Series[float]  # = pandera.Field(nullable=True)
        main_order_id: pt.Series[int] = pandera.Field(nullable=True)
        led_to_order_at: pt.Series[Annotated[pd.DatetimeTZDtype, "ns", "UTC"]] = pandera.Field(nullable=True)
        order_is_active: pt.Series[bool] = pandera.Field(nullable=True)
        :return:
        """
        # todo: test
        base_length = base_pattern['internal_high'] - base_pattern['internal_low']
        if band == 'upper':
            side = OrderSide.Buy.value
            limit_price = base_pattern['internal_high'] + base_pattern[
                'ATR'] * config.base_pattern_order_limit_price_margin_percentage
            stop_loss = base_pattern['internal_low']
            take_profit = base_pattern['internal_high'] + base_length * config.base_pattern_risk_reward_rate
            trigger_price = base_pattern['internal_high']
        else:  # band == 'below':
            raise
        new_signal = SignalDf.new(
            date=self.get_datas().date,
            side=side,
            reference_multi_index=base_pattern_index,
            # base_asset_amount=self.sizer,
            limit_price=limit_price,
            stop_loss=stop_loss,
            take_profit=take_profit,
            trigger_price=trigger_price,
            order_is_active=False,
        )
        self.signal_df = SignalDf.concat(self.signal_df, new_signal)
        log(f"added Signal {new_signal} @ {self.get_datas()}", severity=LogSeverity.DEBUG, stack_trace=False)
        return self.signal_df

    def extract_signals(self) -> SignalDf:
        patterns_to_check = self.overlapping_base_patterns(
            self.no_signal_active_base_patterns(direction='upper', base_patterns=self.base_patterns))
        for index, base_pattern in patterns_to_check.iterrows():
            # todo: test

            if self.get_datas().high > base_pattern['upper_band_activated']:
                self.add_signal(index, base_pattern, band='upper')
                self.loc[index, 'upper_band_signal_generated'] = self.get_datas().date
        patterns_to_check = self.overlapping_base_patterns(
            self.no_signal_active_base_patterns(direction='below', base_patterns=self.base_patterns))
        for index, base_pattern in patterns_to_check.iterrows():
            # todo: test

            if self.get_datas().low > base_pattern['below_band_activated']:
                self.add_signal(index, base_pattern, band='below')
                self.loc[index, 'below_band_signal_generated'] = self.get_datas().date

    def active_signals(self) -> SignalDf:
        # todo: test
        return self.signal_df[
            (self.signal_df['date'] <= self.get_datas().date) &
            ((self.signal_df['end'].isna()) | (self.signal_df['end'] > self.get_datas().date)) &
            (self.signal_df['order_is_active'].inna() | ~self.signal_df['order_is_active'])
            ]

    def ordered_signals(self, signal_df: SignalDf = None) -> SignalDf:
        # todo: test
        if signal_df is None:
            signal_df = self.signal_df
        return signal_df[signal_df['order_is_active'].notna() & signal_df['order_is_active']]

    def update_executed_orders(self):
        # todo: test
        ordered_signals = self.ordered_signals()
        for index, signal in ordered_signals.iterrows():
            if SignalDf.stopped(signal, self.get_datas()):
                log(f'Signal repeated according to Stop-Loss on {SignalDf.repr(index, signal)} @ {self.get_datas()}')
                repeat_signal = signal.copy()
                repeat_signal['original_index'] = index
                repeat_signal['order_is_active'] = False
                repeat_signal['main_order_id'] = pd.NA
                repeat_signal['led_to_order_at'] = pd.NA
                self.signal_df.loc[self.get_datas().date] = repeat_signal
                log(f"repeated Signal:{repeat_signal}@{self.get_datas()} ", severity=LogSeverity.DEBUG, stack_trace=False)
            elif SignalDf.took_profit(signal, self.get_datas()):
                log(f'Took profit on Signal:{SignalDf.repr(index, signal)}@{self.get_datas()}', severity=LogSeverity.DEBUG,
                    stack_trace=False)

    def execute_active_signals(self):
        # todo: test
        if 'main_order_id' not in self.signal_df.columns:
            self.signal_df['main_order_id'] = pd.NA
        active_signals = self.active_signals()
        for start, signal in active_signals.iterrows():
            # Placeholder for actual order placement logic
            # Replace this with your order placement code
            bracket_executor, order_executor = self.executors(signal)
            # Placeholder for setting stop loss and take profit
            execution_type = self.execution_type(signal)
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
                log(f"Signal:{signal} ordered:{main_order_id}", severity=LogSeverity.DEBUG, stack_trace=False)
            else:
                raise Exception('Expected all signals have take_profit')
                # main_order_id = order_executor(size=signal['base_asset_amount'], exectype=execution_type,
                #                                price=signal['stop_loss'],
                #                                limit_price=signal['limit_price'], trigger_price=signal['trigger_price'])
                # self.signal_df.loc[start, 'main_order_id'] = main_order_id

    def executors(self, signal):
        # todo: test
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
        # todo: test
        if pd.notna(signal['stop_loss']) and pd.notna(signal['limit_price']):
            execution_type = ExtendedOrder.StopLimit
        elif pd.notna(signal['stop_loss']):
            execution_type = ExtendedOrder.Stop
        elif pd.notna(signal['limit_price']):
            execution_type = ExtendedOrder.Limit
        else:
            execution_type = ExtendedOrder.Market
        return execution_type

    @staticmethod
    def test_strategy(cash: float, date_range_str: str = None):
        # todo: test
        if date_range_str is None:
            date_range_str = config.processing_date_range
        cerebro = bt.Cerebro()
        cerebro.addstrategy(BasePatternStrategy)
        raw_data = read_base_timeframe_ohlcv(date_range_str)
        data = bt.feeds.PandasData(dataname=raw_data, datetime=None, open=0, close=1, high=2, low=3, volume=4,
                                   openinterest=-1)
        cerebro.adddata(data)
        cerebro.broker.set_cash(cash)
        print('Starting Portfolio Value: %.2f' % cerebro.broker.getvalue())
        cerebro.run()
        print('Final Portfolio Value: %.2f' % cerebro.broker.getvalue())
