from datetime import datetime
from typing import Literal

import backtrader as bt
import pandas as pd
from pandera import typing as pt

from BasePattern import read_multi_timeframe_base_patterns
from Config import config
from PanderaDFM.BasePattern import MultiTimeframeBasePattern, BasePattern
from PanderaDFM.SignalDf import SignalDf
from Strategy.ExtendedStrategy import ExtendedStrategy
from Strategy.order_helper import OrderSide, BracketOrderType
from helper.helper import log_d, measure_time
from ohlcv import read_base_timeframe_ohlcv


class BasePatternStrategy(ExtendedStrategy):
    base_patterns: pt.DataFrame[MultiTimeframeBasePattern]

    def add_signal_source_data(self):
        base_patterns = read_multi_timeframe_base_patterns(self.date_range_str)
        self.base_patterns = base_patterns[~base_patterns['ignore_backtesting']]

    @measure_time
    def __init__(self):
        self.add_signal_source_data()
        super().__init__()
        if self.date_range_str is None:
            raise AssertionError("expected the self.date_range_str be not None!")
        # self.orders_files = open(f'BasePatternStrategy.{config.id}.{self.date_range_str}.csv', 'w')

    # @measure_time
    def notify_order(self, order: bt.Order):
        if order.status == bt.Order.Completed:
            if order.info['custom_type'] == BracketOrderType.Stop.value:
                # a bracket stop-loss order executed.

                # assure closing of the original and profit orders
                original_order, stop_order, profit_order = self.get_order_group(order)
                if (not original_order.status == bt.Order.Completed or
                        not profit_order.status == bt.Order.Canceled):
                    raise NotImplementedError
                # repeat the signal
                index = order.info['signal_index']
                signal = self.signal_df.loc[index]
                log_d(
                    f'repeating Signal  according to Stop-Loss on {SignalDf.to_str(index, signal)} @ {self.candle()}')
                repeat_signal = signal.copy()
                repeat_signal['original_index'] = index
                repeat_signal['order_is_active'] = False
                repeat_signal['original_order_id'] = pd.NA
                # repeat_signal['end'] = pd.NA
                if str(self.signal_df.index.names) != str(
                        ['date', 'reference_date', 'reference_timeframe', 'side']):
                    raise AssertionError(
                        f"Order of signal_df indexes "
                        f"expected to be {str(['date', 'reference_date', 'reference_timeframe', 'side'])} "
                        f"but is {str(self.signal_df.index.names)}")
                new_index = (self.candle().date, index[1], index[2], index[3])
                self.signal_df.loc[new_index] = repeat_signal
                if not self.signal_df.index.is_unique:
                    pass
                log_d(f"repeated Signal:{SignalDf.to_str(self.candle().date, repeat_signal)}@{self.candle().date} ")
            elif order.info['custom_type'] == BracketOrderType.Profit.value:
                # a bracket take-profit order executed.

                # assure closing of the original and stop orders
                original_order, stop_order, profit_order = self.get_order_group(order)
                if (not original_order.status == bt.Order.Completed or
                        not stop_order.status == bt.Order.Canceled):
                    raise NotImplementedError
                index = order.info['signal_index']
                signal = self.signal_df.loc[index]
                log_d(f'Took profit on Signal:{SignalDf.to_str(index, signal)}@{self.candle().date}')
        super().notify_order(order)

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
            high_low = 'high'
            reverse_high_low = 'low'
            side = OrderSide.Buy.value

            def opr(x, y):
                return x + y
        else:  # band == 'below':
            high_low = 'low'
            reverse_high_low = 'high'
            side = OrderSide.Sell.value

            def opr(x, y):
                return x - y

        limit_price = opr(base_pattern[f'internal_{high_low}'],
                          base_pattern['atr'] * config.base_pattern_order_limit_price_margin_percentage)
        stop_loss = base_pattern[f'internal_{reverse_high_low}']
        take_profit = opr(base_pattern[f'internal_{high_low}'], base_length * config.base_pattern_risk_reward_rate)
        trigger_price = base_pattern[f'internal_{high_low}']
        # else:  # band == 'below':
        #     side = OrderSide.Sell.value
        #     limit_price = (base_pattern['internal_low'] -
        #                    base_pattern['atr'] * config.base_pattern_order_limit_price_margin_percentage)
        #     stop_loss = base_pattern['internal_high']
        #     take_profit = base_pattern['internal_low'] - base_length * config.base_pattern_risk_reward_rate
        #     trigger_price = base_pattern['internal_low']

        # size, true_risked_money = self.my_sizer(limit_price=limit_price, sl_price=stop_loss)
        # todo: reference_date and reference_timeframe never been used.
        # todo: use .loc to generate and assign signal in one step.
        if pd.notna(base_pattern['end']):
            effective_end = base_pattern['end']
        else:
            effective_end = base_pattern['ttl']
        if pd.isna(effective_end):
            raise AssertionError("pd.isna(effective_end)")
        # todo: optimize and simplify by removing signals and putting orders instead.

        new_signal = SignalDf.new({  # todo: debug from here
            'date': self.candle().date,
            'original_index': self.candle().date,
            'end': effective_end,
            'side': side,
            'reference_date': pd.to_datetime(base_pattern_date),
            'reference_timeframe': base_pattern_timeframe,
            # 'base_asset_amount': [size],
            'limit_price': limit_price,
            'stop_loss': stop_loss,
            'take_profit': take_profit,
            'trigger_price': trigger_price,
            'trigger_satisfied': False,
            'order_is_active': False,
        })
        self.signal_df = SignalDf.concat(self.signal_df, new_signal)
        log_d(f"added Signal {SignalDf.to_str(new_signal.index[0], new_signal.iloc[0])} @ {self.candle().date}")
        return self.signal_df

    def candle_overlaps_base(self, base_pattern: pt.Series[BasePattern]):
        if self.movement_intersect(base_pattern['internal_low'], base_pattern['internal_high']) > 0:
            return True
        else:
            return False

    # @measure_time
    def extract_signals(self) -> None:
        upper_overlapping_base_patterns = self.overlapping_base_patterns(
            self.no_signal_active_base_patterns(band='upper', base_patterns=self.base_patterns))
        base_pattern: pt.Series[MultiTimeframeBasePattern]
        if len(upper_overlapping_base_patterns) > 0:
            for (timeframe, start), base_pattern in upper_overlapping_base_patterns.iterrows():
                if self.candle_overlaps_base(base_pattern):
                    self.add_signal(timeframe, start, base_pattern, band='upper')
                    if pd.notna(self.base_patterns.loc[(timeframe, start), 'upper_band_signal_generated']):
                        raise AssertionError("pd.notna(self.base_patterns.loc[(timeframe, start), 'upper_band_signa...")
                    self.base_patterns.loc[(timeframe, start), 'upper_band_signal_generated'] = self.candle().date
        below_overlapping_base_patterns = self.overlapping_base_patterns(
            self.no_signal_active_base_patterns(band='below', base_patterns=self.base_patterns))
        if len(below_overlapping_base_patterns) > 0:
            for (timeframe, start), base_pattern in below_overlapping_base_patterns.iterrows():
                if self.candle_overlaps_base(base_pattern):
                    self.add_signal(timeframe, start, base_pattern, band='below')
                    if pd.notna(self.base_patterns.loc[(timeframe, start), 'below_band_signal_generated']):
                        raise AssertionError("pd.notna(self.base_patterns.loc[(timeframe, start), 'below_band_signa...")
                    self.base_patterns.loc[(
                        timeframe,
                        start), 'below_band_signal_generated'] = self.candle().date  # todo: test crashes here


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
    cerebro.adddata(data, name="BTCUSDT")
    # cerebro.addwriter(bt.WriterFile, csv=True)

    cerebro.broker.set_cash(cash)
    cerebro.broker.set_fundmode(True, 0.02)  # 0.02 BTC ~= 1000 USD
    #
    # cerebro.addanalyzer(bt.analyzers.TimeReturn, fund=True)
    # cerebro.addobserver(bt.observers.FundValue)
    # cerebro.addobserver(bt.observers.FundShares)
    # cerebro.addobserver(bt.observers.TimeReturn)
    #
    # cerebro.addanalyzer(bt.analyzers.DrawDown, fund=True)
    # cerebro.addobserver(bt.observers.DrawDown)
    #
    # cerebro.addobserver(bt.observers.Broker)
    #
    # cerebro.addobserver(bt.observers.Benchmark)

    print('Starting Portfolio Value: %.2f' % cerebro.broker.getvalue())
    try:
        result = cerebro.run()
    except Exception as e:
        raise e
    # orders = {}
    # for k, v in result[0].original_orders.items():
    #     orders[k] = v
    # for k, v in result[0].stop_orders.items():
    #     orders[k] = v
    # for k, v in result[0].profit_orders.items():
    #     orders[k] = v
    # # t_order = orders[1]
    # # df = pd.DataFrame(orders.values())
    # print('Final Portfolio Value: %.2f' % cerebro.broker.getvalue())  # todoo: test
    plots = cerebro.plot(style='candle', dpi=900, savefig='plot.pdf')
    # pass
    # a = 1
    # try:
    #     plots[0][0].set_size_inches(19200, 10800)
    #     plots[0][0].savefig(fname=f'c:\\BasePatternStrategy.{config.id}.{date_range_str}.pdf', format='pdf')
    #     # width=19200, height=10800, style='candle', savefig=f'c:\\BasePatternStrategy.{config.id}.{date_range_str}.pdf'
    # except Exception as e:
    #     raise e
