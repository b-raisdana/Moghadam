from typing import Annotated

import pandas as pd
import pandera
from pandera import typing as pt

from data_preparation import d_types, index_fields


class SignalSchema(pandera.DataFrameModel):
    date: pt.Index[Annotated[pd.DatetimeTZDtype, "ns", "UTC"]]
    side: pt.Series[str]  # sell or buy
    base_asset_amount: pt.Series[float]
    limit_price: pt.Series[float]  # = pandera.Field(nullable=True)
    stop_loss: pt.Series[float] = pandera.Field(nullable=True)
    take_profit: pt.Series[float]  # = pandera.Field(nullable=True)
    trigger_price: pt.Series[float]  # = pandera.Field(nullable=True)
    main_order_id: pt.Series[int] = pandera.Field(nullable=True)
    order_is_active: pt.Series[bool] = pandera.Field(nullable=True)


class BasePattern(pandera.DataFrameModel):
    date: pt.Index[Annotated[pd.DatetimeTZDtype, "ns", "UTC"]]
    end: pt.Series[Annotated[pd.DatetimeTZDtype, "ns", "UTC"]] = pandera.Field(nullable=True)
    ttl: pt.Series[Annotated[pd.DatetimeTZDtype, "ns", "UTC"]]
    ATR: pt.Series[float]
    zero_trigger_candle: pt.Series[bool]
    a_pattern_ATR: pt.Series[float]
    a_trigger_ATR: pt.Series[float]
    ATR: pt.Series[float]
    internal_high: pt.Series[float]
    internal_low: pt.Series[float]
    upper_band_activated: pt.Series[Annotated[pd.DatetimeTZDtype, "ns", "UTC"]] = pandera.Field(nullable=True)
    below_band_activated: pt.Series[Annotated[pd.DatetimeTZDtype, "ns", "UTC"]] = pandera.Field(nullable=True)

a3 = [c.__annotations__ for c in BasePattern.__mro__ if hasattr(c, '__annotations__')]
b3 = [c.__annotations__ for c in SignalSchema.__mro__ if hasattr(c, '__annotations__')]
a0 = d_types(BasePattern, include_indexes=True)
b0 = d_types(SignalSchema, include_indexes=True)
a1 = index_fields(BasePattern)
b1 = index_fields(SignalSchema)
