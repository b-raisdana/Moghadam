# from __future__ import annotations
from datetime import datetime
from typing import Annotated

import pandas as pd
import pandera
import pytz
from pandas import Timestamp
from pandera import typing as pt

from Model.BaseTickStructure import BaseTickStructure
from Model.ExtendedDf import ExtendedDf, BasePanderaDFM


class SignalDFM(BasePanderaDFM):
    # from start of exact this candle the signal is valid
    date: pt.Index[Annotated[pd.DatetimeTZDtype, "ns", "UTC"]] = pandera.Field(unique=True,
                                                                               title='date')  # Annotated[pd.DatetimeTZDtype, "ns", "UTC"]]
    # from start of exact this candle the signal is in-valid
    reference_multi_date: pt.Series[Annotated[pd.DatetimeTZDtype, "ns", "UTC"]] = pandera.Field(nullable=True)
    reference_multi_timeframe: pt.Series[str] = pandera.Field(nullable=True)
    end: pt.Series[Annotated[pd.DatetimeTZDtype, "ns", "UTC"]] = pandera.Field(nullable=True)
    # """
    # Limit Orders – regular orders having an amount in base currency (how much you want to buy or sell) and a price in quote currency (for which price you want to buy or sell).
    # Market Orders – regular orders having an amount in base currency (how much you want to buy or sell)
    # Market Buys – some exchanges require market buy orders with an amount in quote currency (how much you want to spend for buying)
    # Trigger Orders – an advanced type of order used to wait for a certain condition on a market and then react automatically: when a triggerPrice is reached, the trigger order gets triggered and then a regular limit price or market price order is placed, that eventually results in entering a position or exiting a position
    # Stop Loss Orders – almost the same as trigger orders, but used to close a position to stop further losses on that position: when the price reaches triggerPrice then the stop loss order is triggered that results in placing another regular limit or market order to close a position at a specific limit price or at market price (a position with a stop loss order attached to it).
    # Take Profit Orders – a counterpart to stop loss orders, this type of order is used to close a position to take existing profits on that position: when the price reaches triggerPrice then the take profit order is triggered that results in placing another regular limit or market order to close a position at a specific limit price or at market price (a position with a take profit order attached to it).
    # StopLoss And TakeProfit Orders Attached To A Position – advanced orders, consisting of three orders of types listed above: a regular limit or market order placed to enter a position with stop loss and/or take profit orders that will be placed upon opening that position and will be used to close that position later (when a stop loss is reached, it will close the position and will cancel its take profit counterpart, and vice versa, when a take profit is reached, it will close the position and will cancel its stop loss counterpart, these two counterparts are also known as "OCO orders – one cancels the other), apart from the amount (and price for the limit order) to open a position it will also require a triggerPrice for a stop loss order (with a limit price if it's a stop loss limit order) and/or a triggerPrice for a take profit order (with a limit price if it's a take profit limit order).
    # """
    # # type: pt.Series[str]  # 'Market', or 'Stop' or 'StopLimit'
    side: pt.Series[str] = pandera.Field(default='buy')  # sell or buy
    # if NaN sizer should give the appropriate size.
    base_asset_amount: pt.Series[float] = pandera.Field(nullable=True, default=pd.NA)
    # the worst acceptable price for order execution.
    limit_price: pt.Series[float] = pandera.Field(nullable=True, default=pd.NA)
    stop_loss: pt.Series[float] = pandera.Field(nullable=True, default=pd.NA)  # , ignore_na=False
    take_profit: pt.Series[float] = pandera.Field(nullable=True, default=pd.NA)
    # the condition direction is reverse of limit direction.
    trigger_price: pt.Series[float] = pandera.Field(nullable=True, default=pd.NA)
    main_order_id: pt.Series[int] = pandera.Field(nullable=True, default=0)
    # led_to_order_at: pt.Series[Annotated[pd.DatetimeTZDtype, "ns", "UTC"]] = pandera.Field(nullable=True)
    order_is_active: pt.Series[bool] = pandera.Field(nullable=True, default=pd.NA)  # , ignore_na=False


class SignalDf(pt.DataFrame[SignalDFM], ExtendedDf):
    schema_data_frame_model = SignalDFM
    _sample_df = pt.DataFrame[SignalDFM]({
        'date': [Timestamp(datetime(year=1980, month=1, day=1, hour=1, minute=1, second=1).replace(tzinfo=pytz.UTC))],
        'reference_multi_date': [
            datetime(year=2023, month=12, day=20, hour=10, minute=11, second=13).replace(tzinfo=pytz.UTC)],
        'side': ['buy'],
        'base_asset_amount': 1.1,
    })
    _empty_obj = None

    @classmethod
    def is_closed(self, signal: pt.Series[SignalDFM], tick: BaseTickStructure):
        if signal['side'] == 'buy':
            if tick.high > signal['take_profit']:
                return True
            if tick.low < signal['stop_loss']:
                return True
        elif signal['side'] == 'sell':
            if tick.low < signal['take_profit']:
                return True
            if tick.high > signal['stop_loss']:
                return True
        else:
            raise Exception(f"Unexpected side({signal['side']}) in {signal} should be 'buy' or 'sell'.")
        return False

    @staticmethod
    def took_profit(signal: pt.Series[SignalDFM], tick: BaseTickStructure) -> bool:
        if signal['side'] == 'buy':
            if tick.high > signal['take_profit']:
                return True
        elif signal['side'] == 'sell':
            if tick.low < signal['take_profit']:
                return True
        else:
            raise Exception(f"Unexpected side({signal['side']}) in {signal} should be 'buy' or 'sell'.")
        return False

    @staticmethod
    def stopped(signal: pt.Series[SignalDFM], tick: BaseTickStructure) -> bool:
        if signal['side'] == 'buy':
            if tick.low < signal['stop_loss']:
                return True
        elif signal['side'] == 'sell':
            if tick.high > signal['stop_loss']:
                return True
        else:
            raise Exception(f"Unexpected side({signal['side']}) in {signal} should be 'buy' or 'sell'.")
        return False


new_signal = SignalDf.new(
    {
        'date': [datetime(2023, 12, 29, 0, 0, tzinfo=pytz.UTC)],
        'side': ['buy'],
        'reference_multi_date': [Timestamp('2023-12-26 11:30:00+0000', tz='UTC')],
        'reference_multi_timeframe': ['15min'],
        'limit_price': [43270.392300534404],
        'stop_loss': [42579.2],
        'take_profit': [43003.40000000002],
        'trigger_price': [42649.9],
        'base_asset_amount': [-1.1],
    }
)
