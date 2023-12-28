from __future__ import annotations
from typing import Annotated

import pandas as pd
import pandera

from pandera import typing as pt

from Config import config
from data_preparation import cast_and_validate, read_file, no_generator
from ohlcv import cache_times


class SignalSchema(pandera.DataFrameModel):
    # from start of exact this candle the signal is valid
    date: pt.Index[Annotated[pd.DatetimeTZDtype, "ns", "UTC"]]
    # from start of exact this candle the signal is in-valid
    end: pt.Index[Annotated[pd.DatetimeTZDtype, "ns", "UTC"]] = pandera.Field(nullable=True)
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


class Signal(pt.DataFrame[SignalSchema]):
    schema_data_frame_model = SignalSchema

    @classmethod
    def cast_and_validate(cls, instance: Signal, inplace: bool = True) -> Signal:
        result: Signal = cast_and_validate(instance, Signal)
        if inplace:
            instance.__dict__ = result.__dict__
            return instance
        else:
            return result

    @classmethod
    def read(cls, date_range_str) -> Signal:
        if date_range_str is None:
            date_range_str = config.processing_date_range
        result: Signal = read_file(date_range_str, 'signal', no_generator, Signal)
        return result

    @classmethod
    def place_order(cls, signal: pt.Series[SignalSchema]):
        side = signal['side']
        base_asset_amount = signal['base_asset_amount']
        limit_price = signal['limit_price']
        stop_loss = signal['stop_loss']
        take_profit = signal['take_profit']
        trigger_price = signal['trigger_price']

        # Placeholder for actual order placement logic
        # Replace this with your order placement code
        if side == 'buy':
            order_executor = self.buy
            bracket_executor = self.sell_bracket
        elif side == 'sell':
            order_executor = self.sell
            bracket_executor = self.buy_bracket
        else:
            raise Exception('Unknown side %s' % side)
        # Placeholder for setting stop loss and take profit
        if pd.notna(stop_loss) and pd.notna(limit_price):
            execution_type = ExtendedOrder.StopLimit
        elif pd.notna(stop_loss):
            execution_type = ExtendedOrder.Stop
        elif pd.notna(limit_price):
            execution_type = ExtendedOrder.Limit
        else:
            execution_type = ExtendedOrder.Market

        if pd.notna(take_profit):
            bracket_executor(size=base_asset_amount, exectype=execution_type, limitprice=take_profit,
                             price=limit_price, stopprice=stop_loss)
            # order_executor(
            #     size=base_asset_amount, exectype=execution_type, price=take_profit, parent=bracket_executor(
            #         limitprice=take_profit, stopprice=stop_loss
            #     )
            # )
        else:
            order_executor(size=base_asset_amount, exectype=execution_type, price=stop_loss,
                           limit_price=limit_price)
