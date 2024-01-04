from enum import Enum

import backtrader as bt
from backtrader import Order

from helper.helper import log_d


class ExtendedOrder(bt.Order):
    trigger_satisfied: bool = False

    def __init__(self, parent, limit_price, stop_loss, take_profit, trigger_price, *args, **kwargs):
        # todo: test
        self.trigger_price = trigger_price
        self.limit_price = limit_price
        self.stop_loss = stop_loss
        self.take_profit = take_profit
        super().__init__(parent, *args, **kwargs)

    def check_trigger(self):
        # todo: test
        if self.isbuy():
            return self.parent.candle().high >= self.trigger_price
        elif self.issell():
            return self.parent.candle().high <= self.trigger_price
        return False

    # def check_trigger(self):
    #     # Implement your trigger condition logic here
    #     # Return True if the condition is satisfied, False otherwise
    #     if self.isbuy():
    #         self.trigger_satisfied |= (self.parent.data.close[0] >= self.trigger_price)
    #     else:  # self.issell()
    #         self.trigger_satisfied |= self.parent.data.close[0] <= self.trigger_price
    #     return self.trigger_satisfied

    def __init__(self, parent, trigger_price, *args, **kwargs):
        # todo: test
        self.trigger_price = trigger_price
        super().__init__(parent, *args, **kwargs)

    def execute(self):
        # todo: test
        # Check trigger condition before executing
        if self.check_trigger():
            # Perform the actual order execution
            return super().execute()
        else:
            # Hold the order if trigger condition is not satisfied
            log_d("Trigger condition not satisfied. Order on hold.")
            return None

    @staticmethod
    def is_open(order: bt.Order):
        # todo: test
        if order.status in [bt.Order.Created, bt.Order.Accepted, bt.Order.Submitted, bt.Order.Partial]:
            return True
        return False


class ExtendedBuyOrder(ExtendedOrder):
    ordtype = Order.Buy


class ExtendedSellOrder(ExtendedOrder):
    ordtype = Order.Sell


class OrderSide(Enum):
    Buy = 'buy'
    Sell = 'sell'


def order_name(cls, order: bt.Order):
    # todo: test
    # TRX<13USDT@0.02
    name = (f"Order"
            f"{('<' if order.isbuy() else '>')}"
            f"{order.size}")
    if order.pricelimit is not None:
        name += f"@{order.pricelimit}"
