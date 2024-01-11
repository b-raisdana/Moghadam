from enum import Enum


class OrderSide(Enum):
    Buy = 'buy'
    Sell = 'sell'


class BracketOrderType(Enum):
    Original = 'original_order'
    Stop = 'stop_order'
    Profit = 'profit'
