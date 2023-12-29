from __future__ import annotations
from typing import Annotated, Tuple, Union, Literal

import pandas as pd
import pandera

from pandera import typing as pt

from Model.BasePattern import BasePattern
from Model.BaseTickStructure import BaseTickStructure
from Model.ExpandedDf import ExpandedDf
from data_preparation import empty_df, index_fields, all_annotations


class SignalSchema(pandera.DataFrameModel):
    # date: pt.Index[Annotated[pd.DatetimeTZDtype, "ns", "UTC"]]
    # end: pt.Series[Annotated[pd.DatetimeTZDtype, "ns", "UTC"]] = pandera.Field(nullable=True)
    # ttl: pt.Series[Annotated[pd.DatetimeTZDtype, "ns", "UTC"]]
    # ATR: pt.Series[float]
    # zero_trigger_candle: pt.Series[bool]
    # a_pattern_ATR: pt.Series[float]
    # a_trigger_ATR: pt.Series[float]
    # ATR: pt.Series[float]
    # internal_high: pt.Series[float]
    # internal_low: pt.Series[float]
    # upper_band_activated: pt.Series[Annotated[pd.DatetimeTZDtype, "ns", "UTC"]] = pandera.Field(nullable=True)
    # below_band_activated: pt.Series[Annotated[pd.DatetimeTZDtype, "ns", "UTC"]] = pandera.Field(nullable=True)

    # from start of exact this candle the signal is valid
    date: pt.Index[Annotated[pd.DatetimeTZDtype, "ns", "UTC"]]
    # from start of exact this candle the signal is in-valid
    # reference_multi_index: pt.Series[Union[Tuple[str, Annotated[pd.DatetimeTZDtype, "ns", "UTC"]]]]
    # end: pt.Index[Annotated[pd.DatetimeTZDtype, "ns", "UTC"]] = pandera.Field(nullable=True)
    """
    Limit Orders – regular orders having an amount in base currency (how much you want to buy or sell) and a price in quote currency (for which price you want to buy or sell).
    Market Orders – regular orders having an amount in base currency (how much you want to buy or sell)
    Market Buys – some exchanges require market buy orders with an amount in quote currency (how much you want to spend for buying)
    Trigger Orders – an advanced type of order used to wait for a certain condition on a market and then react automatically: when a triggerPrice is reached, the trigger order gets triggered and then a regular limit price or market price order is placed, that eventually results in entering a position or exiting a position
    Stop Loss Orders – almost the same as trigger orders, but used to close a position to stop further losses on that position: when the price reaches triggerPrice then the stop loss order is triggered that results in placing another regular limit or market order to close a position at a specific limit price or at market price (a position with a stop loss order attached to it).
    Take Profit Orders – a counterpart to stop loss orders, this type of order is used to close a position to take existing profits on that position: when the price reaches triggerPrice then the take profit order is triggered that results in placing another regular limit or market order to close a position at a specific limit price or at market price (a position with a take profit order attached to it).
    StopLoss And TakeProfit Orders Attached To A Position – advanced orders, consisting of three orders of types listed above: a regular limit or market order placed to enter a position with stop loss and/or take profit orders that will be placed upon opening that position and will be used to close that position later (when a stop loss is reached, it will close the position and will cancel its take profit counterpart, and vice versa, when a take profit is reached, it will close the position and will cancel its stop loss counterpart, these two counterparts are also known as "OCO orders – one cancels the other), apart from the amount (and price for the limit order) to open a position it will also require a triggerPrice for a stop loss order (with a limit price if it's a stop loss limit order) and/or a triggerPrice for a take profit order (with a limit price if it's a take profit limit order).
    """
    # type: pt.Series[str]  # 'Market', or 'Stop' or 'StopLimit'
    side: pt.Series[str]  # sell or buy
    base_asset_amount: pt.Series[float]
    # the worst acceptable price for order execution.
    limit_price: pt.Series[float]  # = pandera.Field(nullable=True)
    stop_loss: pt.Series[float] = pandera.Field(nullable=True)
    take_profit: pt.Series[float]  # = pandera.Field(nullable=True)
    # the condition direction is reverse of limit direction.
    trigger_price: pt.Series[float]  # = pandera.Field(nullable=True)
    main_order_id: pt.Series[int] = pandera.Field(nullable=True)
    # led_to_order_at: pt.Series[Annotated[pd.DatetimeTZDtype, "ns", "UTC"]] = pandera.Field(nullable=True)
    order_is_active: pt.Series[bool] = pandera.Field(nullable=True)

a3 = [c.__annotations__ for c in BasePattern.__mro__ if hasattr(c, '__annotations__')]
b3 = [c.__annotations__ for c in SignalSchema.__mro__ if hasattr(c, '__annotations__')]
a0 = all_annotations(BasePattern, include_indexes=True)
b0 = all_annotations(SignalSchema, include_indexes=True)
a1 = index_fields(BasePattern)
b1 = index_fields(SignalSchema)
# BasePattern.to_schema()
a = empty_df(BasePattern)
# SignalSchema.to_schema()
b = empty_df(SignalSchema)


class SignalDf(pt.DataFrame[SignalSchema], ExpandedDf):
    schema_data_frame_model = SignalSchema

    @classmethod
    def is_closed(self, signal: pt.Series[SignalSchema], tick: BaseTickStructure):
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
    def took_profit(signal: pt.Series[SignalSchema], tick: BaseTickStructure) -> bool:
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
    def stopped(signal: pt.Series[SignalSchema], tick: BaseTickStructure) -> bool:
        if signal['side'] == 'buy':
            if tick.low < signal['stop_loss']:
                return True
        elif signal['side'] == 'sell':
            if tick.high > signal['stop_loss']:
                return True
        else:
            raise Exception(f"Unexpected side({signal['side']}) in {signal} should be 'buy' or 'sell'.")
        return False
