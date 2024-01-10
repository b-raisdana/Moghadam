from typing import Optional, Tuple

import backtrader as bt
import pandas as pd
import pytz
from pandera import typing as pt

from Config import config
from PanderaDFM.SignalDf import SignalDFM, SignalDf
from Strategy.BaseTickStructure import BaseTickStructure
from Strategy.order_helper import BracketOrderType, order_name, OrderSide, add_order_info, order_prices, order_is_open, \
    order_is_closed
from helper.data_preparation import concat
from helper.helper import log_d, measure_time


class ExtendedStrategy(bt.Strategy):
    signal_df: pt.DataFrame[SignalDFM] = None
    orders_df: pd.DataFrame = None
    vault_df: pd.DataFrame = None

    original_orders = {}
    stop_orders = {}
    profit_orders = {}
    archived_orders = {}
    date_range_str: str = None
    # true_risked_money = 0.0
    group_order_counter = None
    broker_initial_cash = None

    @measure_time
    def __init__(self):
        self.signal_df = SignalDf.new()
        self.orders_df = pd.DataFrame()
        self.vault_df = pd.DataFrame()

        self.set_date_range()
        self.bracket_executors = {
            'buy': self.buy_bracket,
            'sell': self.sell_bracket,
        }
        self.next_runs = 0

    # orders_files = None
    def start(self):
        self.broker_initial_cash = self.broker.get_cash()

    def group_order_id(self):
        if self.group_order_counter is None:
            self.group_order_counter = 0
        self.group_order_counter += 1
        return self.group_order_counter

    def add_signal_source_data(self):
        raise NotImplementedError

    @staticmethod
    def risk_size_sizer(limit_price: float, sl_price: float, size: Optional[float] = None) -> Tuple[float, float]:
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

        if not true_risked_money <= config.initial_cash * config.risk_per_order_percent:
            raise AssertionError("True risked money exceeds configured risk percentage")

        return size, true_risked_money

    def post_bracket_order(self, original_order: bt.Order, sl_order: bt.Order, tp_order: bt.Order,
                           signal: pt.Series[SignalDFM], signal_index: pd.MultiIndex):
        group_order_id = self.group_order_id()
        if (
                group_order_id in self.original_orders.keys() or
                group_order_id in self.stop_orders.keys() or
                group_order_id in self.profit_orders.keys()
        ):
            raise AssertionError(f"group_order_id is not unique!")
        original_order = add_order_info(original_order, signal, signal_index, BracketOrderType.Original, group_order_id)
        sl_order = add_order_info(sl_order, signal, signal_index, BracketOrderType.Stop, group_order_id)
        tp_order = add_order_info(tp_order, signal, signal_index, BracketOrderType.Profit, group_order_id)

        self.original_orders[group_order_id] = original_order
        self.stop_orders[group_order_id] = sl_order
        self.profit_orders[group_order_id] = tp_order

        self.signal_df.loc[signal_index, 'original_order_id'] = order_name(original_order)
        self.signal_df.loc[signal_index, 'stop_loss_order_id'] = order_name(sl_order)
        self.signal_df.loc[signal_index, 'take_profit_order_id'] = order_name(tp_order)
        self.signal_df.loc[signal_index, 'end'] = self.candle().date
        # self.signal_df.loc[signal_index, 'led_to_order_at'] = self.candle().date
        self.signal_df.loc[signal_index, 'order_is_active'] = True

        return original_order, sl_order, tp_order

    def spent_money(self) -> float:
        if self.broker_initial_cash is None:
            raise ProcessLookupError("Do not run before start()")
        return self.broker_initial_cash - self.broker.get_cash()

    def allocate_order_cash(self, limit_price: float, sl_price: float, size: float = None) -> float:
        # Allocate 10% of the initial cash
        max_allowed_total_capital_at_risk = config.initial_cash * config.capital_max_total_risk_percentage
        remained_risk_able = max_allowed_total_capital_at_risk - self.spent_money()  # self.true_risked_money
        if size is None:
            size, true_risked_money = self.risk_size_sizer(limit_price, sl_price, size)
        else:
            raise NotImplementedError  # true_risked_money = size * self.candle().close
        if true_risked_money < remained_risk_able:
            # self.true_risked_money += true_risked_money
            return size
        elif remained_risk_able > 0:
            # self.true_risked_money += remained_risk_able
            return remained_risk_able / self.candle().close
        else:
            return 0

        # remaining_cash = config.initial_cash - self.true_risked_money
        # if remaining_cash >= max_allowed_total_capital_at_risk:
        #     size, true_risked_money = self.my_sizer(limit_price, sl_price, size)
        #     self.true_risked_money += true_risked_money
        #     return size
        # else:
        #     # If remaining cash is less than 10% of initial allocated cash, allocate zero
        #     return 0

    def free_order_cash(self, limit_price: float, sl_price: float, size: float):
        # should be handled bby bachtrader!
        # todo: check bracket order cash is not handled.
        # todo: check sell order cash handling
        # todo: never reached!
        # _, true_risked_money = self.risk_size_sizer(limit_price, sl_price, size)  # toddo: test
        # self.true_risked_money -= true_risked_money
        # assert self.true_risked_money > 0, "self.true_risked_money <= 0"
        pass

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

    def get_order_group(self, order: bt.Order) -> (bt.Order, bt.Order, bt.Order,):
        try:
            order_id = order.info['custom_order_id']
            original_order = self.original_orders[order_id]
            stop_order = self.stop_orders[order_id]
            profit_order = self.profit_orders[order_id]
            if order not in [original_order, stop_order, profit_order]:
                raise AssertionError(f"The order: should be one of group members "
                                     f"O:{original_order} S:{stop_order} P:{profit_order}")
        except Exception as e:
            raise e
        return original_order, stop_order, profit_order

    def next_log(self):
        if self.next_runs % 100 == 0:
            log_d(f"Ran {self.next_runs} Next()s reached {self.candle().date}")
        self.next_runs += 1

    def movement_intersect(self, target_low: float, target_high: float):
        movement_low = min(self.candle().low, self.candle(1).low)
        movement_high = max(self.candle().high, self.candle(1).high)
        intersect_high = min(movement_high, target_high)
        intersect_low = max(movement_low, target_low)
        if intersect_high > intersect_low:
            return intersect_high - intersect_low
        else:
            return 0

    def next(self):
        self.next_log()
        if len(self.original_orders) > 0:
            self.verify_triple_oder_status()
        self.extract_signals()
        self.execute_active_signals()

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
        if len(self.original_orders) != len(self.stop_orders):
            raise AssertionError("len(self.original_orders) != len(self.stop_orders)")
        if len(self.original_orders) != len(self.profit_orders):
            raise AssertionError("len(self.original_orders) != len(self.profit_orders)")
        for i in self.original_orders.keys():
            if order_is_open(self.original_orders[i]):
                if not order_is_open(self.stop_orders[i]):
                    AssertionError("!order_is_open(self.stop_orders[i])")
                if not order_is_open(self.profit_orders[i]):
                    AssertionError("!order_is_open(self.profit_orders[i])")
            elif order_is_closed(self.original_orders[i]):
                log_d("Not working: AssertionError")
                if not order_is_closed(self.stop_orders[i]) or not order_is_closed(self.profit_orders[i]):
                    pass
                if not order_is_closed(self.stop_orders[i]):
                    AssertionError("!order_is_closed(self.stop_orders[i])")
                if not order_is_closed(self.profit_orders[i]):
                    AssertionError("!order_is_closed(self.profit_orders[i])")  # todo: test AssertionError

    def stop(self):
        # Close the log file when the strategy is done running
        # self.orders_file.close()
        if self.vault_df is not None:  # todo: test
            self.vault_df.to_csv(f"{self.__class__.__name__}.vault.{config.id}.{self.date_range_str}.csv")
        if self.orders_df is not None:
            self.orders_df.to_csv(f"{self.__class__.__name__}.orders.{config.id}.{self.date_range_str}.csv")
        if self.signal_df is not None:
            self.signal_df.to_csv(f"{self.__class__.__name__}.signals.{config.id}.{self.date_range_str}.csv")
        if self.original_orders is not None:
            for index in self.original_orders.keys():
                t = self.dict_of_list(self.dict_of_order(self.original_orders[index]))
                df = concat(self.orders_df, pd.DataFrame(t, index=[index]))
            df.to_csv(f"{self.__class__.__name__}.original_orders.{config.id}.{self.date_range_str}.csv")
        if self.stop_orders is not None:
            for index in self.stop_orders.keys():
                t = self.dict_of_list(self.dict_of_order(self.stop_orders[index]))
                df = concat(self.orders_df, pd.DataFrame(t, index=[index]))
            df.to_csv(f"{self.__class__.__name__}.stop_orders.{config.id}.{self.date_range_str}.csv")
        if self.profit_orders is not None:
            for index in self.profit_orders.keys():
                t = self.dict_of_list(self.dict_of_order(self.profit_orders[index]))
                df = concat(self.orders_df, pd.DataFrame(t, index=[index]))
            df.to_csv(f"{self.__class__.__name__}.profit_orders.{config.id}.{self.date_range_str}.csv")
        #     for key, order in self.original_orders:
        #         df.loc[key] = pd.DataFrame({self.dict_of_order(order)})
        #     df.to_csv(
        #         f"{self.__class__.__name__}.original_orders.{config.id}.{self.date_range_str}.csv")
        # if self.stop_orders is not None:
        #     df = pd.DataFrame(self.stop_orders)
        #     df.to_csv(f"{self.__class__.__name__}.stop_orders.{config.id}.{self.date_range_str}.csv")
        # if self.profit_orders is not None:
        #     df = pd.DataFrame(self.profit_orders)
        #     df.to_csv(f"{self.__class__.__name__}.profit_orders.{config.id}.{self.date_range_str}.csv")

    @staticmethod
    def dict_of_order(order: bt.Order):
        t_dict = {
            # 'ref': order.ref,
            'side': order.ordtypename(),

            'size': order.size,

            'price': order.params.price,
            'plimit': order.plimit,
            'price_limit': order.params.pricelimit,

            'exectype': order.ExecTypes[order.exectype],
            'status': order.Status[order.status],

            'valid': order.params.valid,
            'type': order.ordtypename(),
            # 'info': str(order.info),

            'triggered': order.triggered,
            'tradeid': order.params.tradeid,
            'trailamount': order.params.trailamount,
            'trailpercent': order.params.trailpercent,
            'transmit': order.params.transmit,
            'histnotify': order.params.histnotify,
            'oco': order.params.oco,
            'parent': order.params.parent,
            'simulated': order.params.simulated,
            'plen': order.plen,

        }
        # dict = {k: v for k, v in _object.__dict__.items() if not k.startswith('_')}
        return t_dict

    @staticmethod
    def dict_of_list(input_dict):
        result = {k: [v] for k, v in input_dict.items()}
        return result

    def log_order(self, order: bt.Order, index = None):
        if index is None:
            index = order.ref
        t_dict = self.dict_of_list(self.dict_of_order(order))
        self.orders_df = concat(self.orders_df, pd.DataFrame(t_dict, index=[index]))
        # pd.DataFrame({**_dict}, index=order.ref)
        self.orders_df.to_csv(f"{self.__class__.__name__}.orders.{config.id}.{self.date_range_str}.csv")

    def log_vault(self):
        _dict = {
            'cash': self.broker.get_cash(),
            'fund_shares': self.broker.get_fundshares(),
            'fund_value': self.broker.get_fundvalue(),
        }
        if len(self.getdatanames()) > 1:
            raise NotImplementedError
        self.broker: bt.BackBroker
        # for index in len(self.datas):
        #     asset = list_of_assets[index]
        asset = self.getdatanames()[0]
        _dict[f"{asset}_value"] = self.broker.get_value()
        position = self.broker.getposition(self.datas[0])
        _dict[f"{asset}_position_size"] = position.size
        _dict[f"{asset}_position_price"] = position.price
        _dict[f"{asset}_position_adjbase"] = position.adjbase
        _dict[f"{asset}_position_price_orig"] = position.price_orig
        _dict[f"{asset}_position_upclosed"] = position.upclosed
        _dict[f"{asset}_position_updt"] = position.updt
        _dict[f"{asset}_position_upopened"] = position.upopened
        # self.broker_history_dfloc[len(self.vault_df)] = pd.DataFrame(dict)
        # self.vault_df.to_csv(f"{self.__class__.__name__}.vault.{config.id}.{self.date_range_str}.csv")
        _dict = self.dict_of_list(_dict)
        self.vault_df = concat(self.vault_df,
                               pd.DataFrame(_dict,
                                            index=[self.candle().date]))  # pd.DataFrame({**_dict}, index=order.ref)
        self.vault_df.to_csv(f"{self.__class__.__name__}.vault.{config.id}.{self.date_range_str}.csv")

    # @measure_time
    def notify_order(self, order: bt.Order):
        self.log_order(order)
        self.log_vault()
        if not (
                order_name(order) in self.signal_df['original_order_id'].dropna().tolist() or
                order_name(order) in self.signal_df['stop_loss_order_id'].dropna().tolist() or
                order_name(order) in self.signal_df['take_profit_order_id'].dropna().tolist()
        ):
            raise AssertionError(f"{order_name(order)} not found in tracked orders!")
        if order.isbuy():
            size_positiver = 1
        else:
            size_positiver = -1
        if not ((order.size * size_positiver) > 0):
            raise AssertionError("not ((order.size * size_positiver) > 0)")
        if not ((order.size * size_positiver) > 1 / 43000):  # , "Order size less than 1 dollar"
            raise AssertionError("not ((order.size * size_positiver) > 1 / 43000)")
        # if order.status in [order.Completed, order.Partial, order.Expired, order.Canceled, order.Rejected]:
        #     # cash freeing expected to be handled by backtrader!
        #     # if order.info['custom_type'] == BracketOrderType.Original.value:
        #     #     limit_price, stop_loss, take_profit = order_prices(order)  # toddo: test
        #     #     # self.free_order_cash(limit_price, stop_loss, order.size)
        #     # else:
        #     #     pass
        if order.status in [order.Submitted, order.Accepted]:
            # Order has been submitted or accepted
            log_d(f"Order:{order_name(order)} Submitted")
        elif order.status in [order.Completed]:
            # Order has been completed (executed)
            self.signal_df.loc[
                self.signal_df['original_order_id'] == order_name(order), 'order_is_active'] = False
            self.signal_df.loc[self.signal_df['original_order_id'] == order_name(order), 'original_order_id'] = pd.NA
            log_d(f"Order:{order_name(order)} Completed")
            # if order.ref == 7:
            #     pass  # toddo: test crashes after here.
        elif order.status in [order.Partial]:
            log_d(f"Order:{order_name(order)} Partially executed")  # todo: test
        elif order.status in [order.Expired]:
            log_d(f"Order:{order_name(order)} Expired")
        elif order.status in [order.Canceled]:
            log_d(f"Order:{order_name(order)} Canceled")
        elif order.status in [order.Rejected]:
            raise Exception(f"Order:{order_name(order)} Rejected")  # todo: test
        else:
            raise Exception(f"Order:{order_name(order)} Unexpected status {order.status}")  # todo: test

    def ordered_signals(self, signal_df: SignalDf = None) -> pt.DataFrame[SignalDFM]:
        if signal_df is None:
            signal_df = self.signal_df
        return signal_df[signal_df['order_is_active'].notna() & signal_df['order_is_active']]

    def active_signals(self) -> pt.DataFrame[SignalDFM]:
        """
        return all signals which the self.candle()date is between signal start (signal.index) and signal end.
        :return:
        """
        try:
            if 'end' not in self.signal_df.columns:
                # self.signal_df['end'] = pd.Series(dtype='datetime64[ns, UTC]')
                pass
            result = self.signal_df[
                (self.signal_df.index.get_level_values(level='date') <= self.candle().date) &
                (
                    # ('end' not in self.signal_df.columns) |
                        (self.signal_df['end'].isna()) |
                        (self.signal_df['end'] > self.candle().date)) &
                (self.signal_df['order_is_active'].isna() | ~self.signal_df['order_is_active'])
                ]
        except Exception as e:
            raise e
        return result

    # def update_ordered_signals(self):
    #     ordered_signals = self.ordered_signals()
    #     for index, signal in ordered_signals.iterrows():
    #         if SignalDf.stopped(signal, self.candle()):  # toddo: test
    #             log_d(f'Signal repeated according to Stop-Loss on {SignalDf.to_str(index, signal)} @ {self.candle()}')
    #             repeat_signal = signal.copy()
    #             repeat_signal['original_index'] = index
    #             repeat_signal['order_is_active'] = False
    #             repeat_signal['original_order_id'] = pd.NA
    #             repeat_signal['end'] = pd.NA
    #             self.signal_df.loc[self.candle().date] = repeat_signal
    #             log_d(f"repeated Signal:{SignalDf.to_str(self.candle().date, repeat_signal)}@{self.candle().date} ")
    #         elif SignalDf.took_profit(signal, self.candle()):
    #             log_d(f'Took profit on Signal:{SignalDf.to_str(index, signal)}@{self.candle().date}')
    #         # elif SignalDf.expired(signal, self.candle()):
    #         #     log(f'Signal:{SignalDf.to_str(signal)}@{self.candle()} Expired')

    # def update_signal_trigger_status(self, side: OrderSide):
    #     side_indexes = self.signal_df[self.signal_df['side'] == side.value].index
    #     if side == OrderSide.Buy:
    #         self.signal_df.loc[side_indexes, 'trigger_satisfied'] = \
    #             (self.signal_df.loc[side_indexes, 'trigger_satisfied'] |
    #              (self.signal_df.loc[side_indexes, 'trigger_price'] < self.candle().high))
    #     else:  # side == OrderSide.Sell:
    #         self.signal_df.loc[side_indexes, 'trigger_satisfied'] = \
    #             (self.signal_df.loc[side_indexes, 'trigger_satisfied'] |
    #              (self.signal_df.loc[side_indexes, 'trigger_price'] < self.candle().high))
    #     assert all(self.signal_df.loc[self.signal_df['trigger_price'].isna(), 'trigger_satisfied'] == True)

    def executable_signals(self) -> pt.DataFrame[SignalDFM]:
        return self.active_signals()
        # self.update_signal_trigger_status(OrderSide.Buy)
        # self.update_signal_trigger_status(OrderSide.Sell)
        # active_signals = self.active_signals()  # .copy()
        # # if len(active_signals) > 0:        #     pass
        # trigger_signal_indexes = active_signals[active_signals['trigger_satisfied']].index
        # # if len(trigger_signal_indexes) > 0:        #     pass
        # buy_signal_indexes = active_signals[active_signals['side'] == 'buy'].index
        # # if len(buy_signal_indexes) > 0:        #     pass
        # buy_trigger_signal_indexes = trigger_signal_indexes.intersection(buy_signal_indexes)
        # # if len(buy_trigger_signal_indexes) > 0:        #     pass
        # sell_signal_indexes = active_signals.index.difference(buy_signal_indexes)
        # # if len(sell_signal_indexes) > 0:        #     pass
        # sell_trigger_signal_indexes = trigger_signal_indexes.intersection(sell_signal_indexes)
        # # if len(sell_trigger_signal_indexes) > 0:        #     pass
        # executable_signal_indexes = buy_trigger_signal_indexes.union(sell_trigger_signal_indexes)
        # # if len(executable_signal_indexes) > 0:        #     pass
        # return active_signals.loc[executable_signal_indexes]

    def idx_in_indexes(self, df: pd.DataFrame, ):
        if not hasattr(df.index, 'names'):
            raise AssertionError("df must have MultiIndex.")

    # @measure_time
    def execute_active_signals(self):
        _executable_signals = self.executable_signals()
        # for signal_index, signal in self.executable_signals().iterrows():
        for idx in range(len(_executable_signals)):
            signal = _executable_signals.iloc[idx]
            signal_side = _executable_signals.index.get_level_values(level='side')[idx]
            signal_index = _executable_signals.index[idx]
            if pd.notna(_executable_signals.iloc[idx]['take_profit']):
                original_order: bt.Order
                try:
                    size = self.allocate_order_cash(_executable_signals.iloc[idx]['limit_price'],
                                                    signal['stop_loss'])  # signal['base_asset_amount'])
                    if size == 0:
                        continue
                except Exception as e:
                    raise e
                if pd.notna(signal['limit_price']) and pd.notna(signal['stop_loss']):
                    if (
                            pd.isna(signal['trigger_price']) or
                            ((signal_side == OrderSide.Buy.value) and (self.candle().high > signal['trigger_price']))
                            or
                            ((signal_side == OrderSide.Sell.value) and (self.candle().low < signal['trigger_price']))
                    ):
                        original_order, stop_order, profit_order = \
                            self.bracket_executors[signal_side](size=size,
                                                                exectype=bt.Order.Limit,
                                                                limitprice=signal['take_profit'],
                                                                price=signal['limit_price'],
                                                                stopprice=signal['stop_loss'],
                                                                valid=signal['end'],
                                                                )
                    else:  # the trigger_price is not satisfied
                        original_order, stop_order, profit_order = \
                            self.bracket_executors[signal_side](size=size,
                                                                exectype=bt.Order.StopLimit,
                                                                price=signal['trigger_price'],
                                                                pprice=signal['limit_price'],
                                                                limitprice=signal['take_profit'],
                                                                stopprice=signal['stop_loss'],
                                                                valid=signal['end'],
                                                                )
                else:
                    raise NotImplementedError
                self.post_bracket_order(original_order, stop_order, profit_order, signal, signal_index)
                log_d(f"Signal:{SignalDf.to_str(signal_index, signal)} Ordered: "
                      f"O:{order_name(original_order)} S:{order_name(stop_order)} P:{order_name(profit_order)}")
            else:
                raise NotImplementedError
