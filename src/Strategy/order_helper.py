from enum import Enum

import backtrader as bt


# switching to backrader
class OrderSide(Enum):
    Buy = 'buy'
    Sell = 'sell'


class OrderBracketType(Enum):
    Original = 'original_order'
    Stop = 'stop_order'
    Profit = 'profit'


def order_name(order: bt.Order):
    # TRX<13USDT@0.02
    """
    tojoin.append('CommInfo: {}'.format(self.comminfo))
    tojoin.append('End of Session: {}'.format(self.dteos))
    tojoin.append('Alive: {}'.format(self.alive()))

    :param order:
    :return:
    """
    name = (
        f"{order.ordtypename()}"
        # f"Order"  
        f"{('<' if order.isbuy() else '>')}"
        f"{order.size:.4f}"
        # if order.params.pricelimit is not None:
        f"@{order.price:.2f}"
        f"Ref{order.ref}"
        # f"{'Alive' if order.alive() else 'Dead'}/{order.getstatusname()}"
        f"{order.ExecTypes[order.exectype]}"
    )
    if order.plimit is not None:
        name += f"PL{order.pricelimit}"
    if order.trailamount is not None:
        name += f"TR{order.trailamount}/{order.trailpercent}"
    return name


def add_order_info(order: bt.Order, signal, signal_index, order_type: OrderBracketType, order_id):
    order.addinfo(custom_order_id=order_id)
    order.addinfo(signal=signal)
    order.addinfo(signal_index=signal_index)
    order.addinfo(custom_type=order_type.value)
    return order


def order_prices(order: bt.Order):
    return order.info['limit_price'], order.info['stop_loss'], order.info['take_profit'],


# class ExtendedBuyOrder(ExtendedOrder):
#     ordtype = bt.BuyOrder
#
#
# class ExtendedSellOrder(ExtendedOrder):
#     ordtype = bt.SellOrder


def order_is_open(order):
    if order.status in [bt.Order.Created, bt.Order.Accepted, bt.Order.Submitted, bt.Order.Partial]:  # todo: test
        return True
    return False


def order_is_closed(order):
    if order.status in [bt.Order.Canceled, bt.Order.Expired, bt.Order.Rejected]:  # todo: test
        return True
    return False
