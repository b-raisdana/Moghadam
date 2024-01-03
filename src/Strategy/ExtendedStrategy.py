from datetime import datetime
from typing import Dict, Optional, Tuple

import backtrader as bt
import numpy as np
import pandas as pd
import pytz
from pandera import typing as pt

from Config import config
from PanderaDFM.SignalDf import SignalDFM, SignalDf
from Strategy.BaseTickStructure import BaseTickStructure
from Strategy.ExtendedOrder import OrderBracketType, ExtendedOrder
from helper.helper import log, log_d


class ExtendedStrategy(bt.Strategy):
    signal_df: pt.DataFrame[SignalDFM] = SignalDf.new()
    original_orders: Dict[np.float64, bt.Order] = {}
    stop_orders: Dict[np.float64, bt.Order] = {}
    profit_orders: Dict[np.float64, bt.Order] = {}
    archived_orders: Dict[np.float64, bt.Order] = {}
    date_range_str: str = None
    true_risked_money = 0.0

    @staticmethod
    def my_sizer(limit_price: float, sl_price: float, size: Optional[float] = None) -> Tuple[float, float]:
        """
        Calculate position size based on limit and stop prices.

        Parameters:
        - limit_price (float): The limit price of the order.
        - sl_price (float): The stop-loss price of the order.
        - size (float, optional): The desired position size. If not provided, it will be calculated based on risk percentage.

        Returns:
        Tuple[float, float]: A tuple containing the calculated position size and the true risked money.

        Raises:
        AssertionError: If the true risked money exceeds the configured risk percentage.
        """
        sl_size = abs(sl_price - limit_price)

        if size is None:
            risk_per_order_size = config.initial_cash * config.risk_per_order_percent  # 10$
            size = risk_per_order_size / sl_size

        true_risked_money = size * sl_size

        assert true_risked_money <= config.initial_cash * config.risk_per_order_percent, \
            "True risked money exceeds configured risk percentage"

        return size, true_risked_money

    def post_bracket_order(self, original_order, stop_order, profit_order, signal_start: datetime,
                           signal: pt.Series[SignalDFM]):
        custom_order_id = np.float64(datetime.now(tz=pytz.UTC).timestamp())
        original_order.add_order_info(signal, signal_start, OrderBracketType.Original, custom_order_id)
        stop_order.add_order_info(signal, signal_start, OrderBracketType.Stop, custom_order_id)
        profit_order.add_order_info(signal, signal_start, OrderBracketType.Profit, custom_order_id)
        self.original_orders[custom_order_id] = original_order
        self.stop_orders[custom_order_id] = stop_order
        self.profit_orders[custom_order_id] = profit_order
        return original_order, stop_order, profit_order

    # @staticmethod
    # def pre_bracket_order(price, stopprice, limitprice, **kwargs):
    #     assert 'trigger_price' in kwargs.keys()
    #     kwargs['limit_price'] = price
    #     kwargs['stop_loss_price'] = stopprice
    #     kwargs['take_profit_price'] = limitprice
    #     return kwargs

    # def post_bracket_order(self, original_order, stop_order, profit_order, **kwargs):
    #     assert 'signal' in kwargs.keys(), "Expected to have signal"
    #     assert 'signal_start' in kwargs.keys(), "Expected to have signal_start"
    #     signal = kwargs['signal']
    #     signal_start = kwargs['signal_start']
    #     custom_order_id = np.float64(datetime.now(tz=pytz.UTC).timestamp())
    #     original_order.add_order_info(signal, signal_start, OrderBracketType.Original, custom_order_id)
    #     stop_order.add_order_info(signal, signal_start, OrderBracketType.Stop, custom_order_id)
    #     profit_order.add_order_info(signal, signal_start, OrderBracketType.Profit, custom_order_id)
    #     self.original_orders[custom_order_id] = original_order
    #     self.stop_orders[custom_order_id] = stop_order
    #     self.profit_orders[custom_order_id] = profit_order
    #     return original_order, stop_order, profit_order

    # def buy_bracket(self, price=None, stopprice=None, limitprice=None, **kwargs):
    #     # todo: test
    #     # add stop_price and limit_price to kwargs to be added into the orders info and make it possible to reference in
    #     # future
    #     kwargs = self.pre_bracket_order(price, stopprice, limitprice, **kwargs)
    #     original_order, stop_order, profit_order = super().buy_bracket(self, price=price, stopprice=stopprice,
    #                                                                    limitprice=limitprice, **kwargs)
    #     original_order, stop_order, profit_order = self.post_bracket_order(original_order, stop_order, profit_order,
    #                                                                        **kwargs)
    #     return [original_order, stop_order, profit_order]

    # def sell_bracket(self, data=None, size=None, price=None, plimit=None,
    #                  exectype=bt.Order.Limit, valid=None, tradeid=0,
    #                  trailamount=None, trailpercent=None, oargs={},
    #                  stopprice=None, stopexec=bt.Order.Stop, stopargs={},
    #                  limitprice=None, limitexec=bt.Order.Limit, limitargs={},
    #                  **kwargs):
    #     raise NotImplementedError
    #     # todo: test
    #     # add stop_price and limit_price to kwargs to be added into the orders info and make it possible to reference in
    #     # future
    #     kwargs = self.pre_bracket_order(price, stopprice, limitprice, **kwargs)
    #     original_order, stop_order, profit_order = super().sell_bracket(self, data, size, price, plimit, exectype,
    #                                                                     valid, tradeid, trailamount,
    #                                                                     trailpercent, oargs, stopprice, stopexec,
    #                                                                     stopargs, limitprice, limitexec,
    #                                                                     limitargs, **kwargs)
    #     original_order, stop_order, profit_order = self.post_bracket_order(original_order, stop_order, profit_order,
    #                                                                        **kwargs)
    #     return [original_order, stop_order, profit_order]

    def allocate_order_cash(self, limit_price: float, sl_price: float) -> float:
        # todo: test
        # Allocate 1/100 of the initial cash
        risk_amount = config.initial_cash() * config.order_max_capital_risk_precentage
        remaining_cash = config.initial_cash - self.true_risked_money
        if remaining_cash >= risk_amount:
            size, true_risked_money = self.my_sizer(limit_price, sl_price)
            self.true_risked_money += true_risked_money
            return size
        else:
            # If remaining cash is less than 10% of initial allocated cash, allocate zero
            return 0

    def free_order_cash(self, limit_price: float, sl_price: float, size: float):
        # todo: test
        _, true_risked_money = self.my_sizer(limit_price, sl_price, size)
        self.true_risked_money -= true_risked_money

    def candle(self, backward_index: int = 0) -> BaseTickStructure:
        return BaseTickStructure(
            date=bt.num2date(self.datas[0].datetime[backward_index]).replace(tzinfo=pytz.UTC),
            close=self.datas[0].close[backward_index],
            open=self.datas[0].open[backward_index],
            high=self.datas[0].high[backward_index],
            low=self.datas[0].low[backward_index],
            volume=self.datas[0].volume[backward_index],
        )

    def set_date_range(self, date_range_str: str = None):
        if date_range_str is None:
            date_range_str = config.processing_date_range
        self.date_range_str = date_range_str
        # self.start_datime, self.end = date_range(date_range_str)

    def __init__(self):
        self.signal_df = SignalDf.new()
        self.set_date_range()
        self.bracket_executors = {
            'buy': self.buy_bracket,
            'sell': self.sell_bracket,
        }

    def get_order_group(self, order: bt.Order) -> (bt.Order, bt.Order, bt.Order,):
        assert hasattr(order.info, 'order_id'), "Expected order_id being found in order.info"
        order_id = order.info['order_id']
        original_order = self.original_orders[order_id]
        stop_order = self.stop_orders[order_id]
        profit_order = self.profit_orders[order_id]
        return original_order, stop_order, profit_order

    def next(self):
        if len(self.original_orders) > 0:
            self.verify_triple_oder_status()
        # self.update_bases() the base patterns are prepared before!
        self.extract_signals()
        # self.update_orders()
        self.update_ordered_signals()
        self.execute_active_signals()
        super()

    def notify_order(self, order: bt.Order):
        # todo: test
        assert order.__str__() in self.signal_df['order_id']
        assert all(self.signal_df.loc[self.signal_df['order_id'] == order.__str__(), 'order_is_active']
                   == True)
        assert order.size > 0
        assert order.size > 1 / 43000, "Order size less than 1 dollar"
        if order.status in [order.Completed, order.Partial, order.Expired, order.Canceled, order.Rejected]:
            if order.info['custom_type'] == OrderBracketType.Original.value:
                limit_price, stop_loss, take_profit = self.get_order_prices(order)
                self.free_order_cash(limit_price, stop_loss, order.size)
            else:
                pass
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

    def active_signals(self) -> pt.DataFrame[SignalDFM]:
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

    def update_ordered_signals(self):
        ordered_signals = self.ordered_signals()
        for index, signal in ordered_signals.iterrows():
            # todo: test
            if SignalDf.stopped(signal, self.candle()):
                log_d(f'Signal repeated according to Stop-Loss on {SignalDf.to_str(index, signal)} @ {self.candle()}')
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

    def executable_signals(self) -> pt.DataFrame[SignalDFM]:
        active_signals = self.active_signals()  # .copy()
        if len(active_signals) > 0:
            pass
        trigger_signal_indexes = active_signals[active_signals['trigger_price'].notna()].index
        if len(trigger_signal_indexes) > 0:
            pass
        no_trigger_signal_indexes = active_signals.index.difference(trigger_signal_indexes)
        if len(no_trigger_signal_indexes) > 0:
            raise NotImplemented  # todo: test

        buy_signal_indexes = active_signals[active_signals['side'] == 'buy'].index
        if len(buy_signal_indexes) > 0:
            pass
        buy_trigger_signal_indexes = trigger_signal_indexes.intersection(buy_signal_indexes)
        if len(buy_trigger_signal_indexes) > 0:
            pass  # todo: test

        sell_signal_indexes = active_signals.index.difference(buy_signal_indexes)
        if len(sell_signal_indexes) > 0:
            pass  # todo: test
        sell_trigger_signal_indexes = trigger_signal_indexes.intersection(sell_signal_indexes)
        if len(sell_trigger_signal_indexes) > 0:
            pass  # todo: test

        trigger_signal_indexes = buy_trigger_signal_indexes.union(sell_trigger_signal_indexes)
        if len(trigger_signal_indexes) > 0:
            pass  # todo: test
        executable_signal_indexes = trigger_signal_indexes.union(no_trigger_signal_indexes)
        if len(executable_signal_indexes) > 0:
            pass  # todo: test
        return active_signals.loc[executable_signal_indexes]

    def execute_active_signals(self):
        # todo: test
        for start, signal in self.executable_signals().iterrows():
            # bracket_executor, order_executor = self.executors(signal)
            execution_type = SignalDf.execution_type(signal)
            if pd.notna(signal['take_profit']):
                original_order: bt.Order
                size = signal['base_asset_amount'] if pd.notna(signal['base_asset_amount']) else None
                kwargs = self.pre_bracket_order(limitprice=signal['take_profit'],
                                                price=signal['limit_price'],
                                                stopprice=signal['stop_loss'])
                original_order, stop_order, profit_order = \
                    self.bracket_executors[signal['side']](size=size,
                                                           exectype=execution_type,
                                                           limitprice=signal['take_profit'],
                                                           price=signal['limit_price'],
                                                           stopprice=signal['stop_loss'])
                self.post_bracket_order(original_order, stop_order, profit_order, **kwargs)

                log_d(f"Signal:{signal} ordered "
                      f"M:{original_order.__str__()} S:{stop_order.__str__()} P:{profit_order.__str__()}")
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
         - length of self.original_orders and alef.stop_orders and self.profit_orders are equal
         - if the self.original_orders.status is open the same index in both other lists
         have the same status
         - if the self.original_orders.status in closed (by any way) the same index in both other lists are closed
         (canceled) also
        :return:
        '''
        # todo: test
        assert len(self.original_orders) == len(self.stop_orders)
        assert len(self.original_orders) == len(self.profit_orders)
        for index, order in self.original_orders:
            if order.is_open():
                assert self.stop_orders[index].is_open()
                assert self.profit_orders[index].is_open()

    def executors(self, signal):
        return {
            'buy': self.buy
        }
        if signal['side'] == 'buy':
            order_executor = self.buy
            bracket_executor = self.buy_bracket
        elif signal['side'] == 'sell':
            # todo: test
            raise NotImplemented
            order_executor = self.sell
            bracket_executor = self.sell_bracket
            self.check_trigger()
        else:
            raise Exception('Unknown side %s' % signal['side'])
        return bracket_executor, order_executor
