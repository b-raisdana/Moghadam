from typing import Annotated

import pandas as pd
import pandera
from pandas import Timestamp
from pandera import typing as pt


class BullBearSide(pandera.DataFrameModel):
    date: pt.Index[Annotated[pd.DatetimeTZDtype, "ns", "UTC"]]  # start
    bull_bear_side: pt.Series[str]
    end: pt.Series[Annotated[pd.DatetimeTZDtype, "ns", "UTC"]]
    internal_high: pt.Series[float]
    high_time: pt.Series[Annotated[pd.DatetimeTZDtype, "ns", "UTC"]]
    internal_low: pt.Series[float]
    low_time: pt.Series[Annotated[pd.DatetimeTZDtype, "ns", "UTC"]]
    movement: pt.Series[float]
    # strength: pt.Series[float]
    ATR: pt.Series[float]
    # duration: pt.Series[timedelta]
    movement_start_value: pt.Series[float] = pandera.Field(nullable=True)
    movement_end_value: pt.Series[float] = pandera.Field(nullable=True)
    movement_start_time: pt.Series[Annotated[pd.DatetimeTZDtype, "ns", "UTC"]] = pandera.Field(nullable=True)
    movement_end_time: pt.Series[Annotated[pd.DatetimeTZDtype, "ns", "UTC"]] = pandera.Field(nullable=True)



