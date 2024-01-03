from enum import Enum

import backtrader as bt
from backtrader import Order

from helper.helper import log_d


# switching to backrader
class OrderSide(Enum):
    Buy = 'buy'
    Sell = 'sell'


class OrderBracketType(Enum):
    Original = 'original_order'
    Stop = 'stop_order'
    Profit = 'profit'


def order_name(order: bt.Order):
    # todo: test
    # TRX<13USDT@0.02
    """
    tojoin.append('CommInfo: {}'.format(self.comminfo))
    tojoin.append('End of Session: {}'.format(self.dteos))
    tojoin.append('Alive: {}'.format(self.alive()))

    :param order:
    :return:
    """

    name = (f"Order"
            f"{('<' if order.isbuy() else '>')}"
            f"T{order.ordtypename()}"
            f"{order.size}"
            # if order.params.pricelimit is not None:
            f"@{order.price}"
            # if order.plimit is not None:
            f"PL{order.pricelimit}"
            f"EX{order.getordername()}"
            f"TR{order.trailamount}/{order.trailpercent}"
            f"ST{order.getstatusname()}"
            f"Ref{order.ref}"
            f"{'A' if order.alive() else ''}"
            )
    return name


class ExtendedOrder(bt.order.Order):
    # trigger_satisfied: bool = False
    # limit_price: float = None
    # stop_loss_price: float = None
    # take_profit_price: float = None

    # def check_trigger(self):
    #     # todo: test
    #     assert self.trigger_price is not None
    #     if self.isbuy():
    #         return self.parent.candle().high >= self.trigger_price
    #     elif self.issell():
    #         return self.parent.candle().high <= self.trigger_price
    #     return False
    #
    # def execute(self, dt, size, price, closed, closedvalue, closedcomm, opened, openedvalue, openedcomm, margin, pnl,
    #             psize, pprice):
    #     # todo: test
    #     # Check trigger condition before executing
    #     if self.trigger_satisfied or self.check_trigger():
    #         self.trigger_satisfied = True
    #         # Perform the actual order execution
    #         return super().execute(dt, size, price,
    #                                closed, closedvalue, closedcomm,
    #                                opened, openedvalue, openedcomm,
    #                                margin, pnl,
    #                                psize, pprice)
    #     else:
    #         # Hold the order if trigger condition is not satisfied
    #         log_d("Trigger condition not satisfied yet. Order on hold.")
    #         return None

    def is_open(self):
        # todo: test
        if self.status in [bt.Order.Created, bt.Order.Accepted, bt.Order.Submitted, bt.Order.Partial]:
            return True
        return False

    def add_order_info(self, signal, signal_index, order_type: OrderBracketType, order_id):
        self.addinfo(custom_order_id=order_id)
        self.addinfo(signal=signal)
        self.addinfo(signal_index=signal_index)
        self.addinfo(custom_type=order_type.value)

    @classmethod
    def get_order_prices(cls, order: bt.Order):
        return order.info['limit_price'], order.info['stop_loss'], order.info['take_profit'],

# class ExtendedBuyOrder(ExtendedOrder):
#     ordtype = bt.BuyOrder
#
#
# class ExtendedSellOrder(ExtendedOrder):
#     ordtype = bt.SellOrder
