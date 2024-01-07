from enum import Enum

import backtrader as bt
import pandas as pd
from pandera import typing as pt

from PanderaDFM.SignalDf import SignalDFM


# switching to backrader
class OrderSide(Enum):
    Buy = 'buy'
    Sell = 'sell'


class BracketOrderType(Enum):
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


"""
date: pt.Index[Annotated[pd.DatetimeTZDtype, "ns", "UTC"]]  # = pandera.Field(title='date')
reference_date: pt.Index[Annotated[pd.DatetimeTZDtype, "ns", "UTC"]]
reference_timeframe: pt.Index[str]
side
"""


def add_order_info(order: bt.Order, signal: pt.Series[SignalDFM], signal_index: pd.MultiIndex,
                   order_type: BracketOrderType, order_id):
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
    return order.status in [bt.Order.Created, bt.Order.Accepted, bt.Order.Submitted, bt.Order.Partial]


def order_is_closed(order):
    return order.status in [bt.Order.Canceled, bt.Order.Expired, bt.Order.Rejected]
