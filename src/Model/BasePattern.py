from datetime import datetime
from typing import Annotated

import pandas as pd
import pandera
from pandera import typing as pt

from Model.MultiTimeframe import MultiTimeframe


class BasePattern(pandera.DataFrameModel):
    date: pt.Index[Annotated[pd.DatetimeTZDtype, "ns", "UTC"]]
    end: pt.Series[Annotated[pd.DatetimeTZDtype, "ns", "UTC"]]
    ttl: pt.Series[Annotated[pd.DatetimeTZDtype, "ns", "UTC"]]
    internal_high: pt.Series[float]
    internal_low: pt.Series[float]

    @classmethod
    def repr(cls, _start: datetime, _trend):
        text = f'BASE: ' \
               f'{_start.strftime("%H:%M")}-{_trend["end"].strftime("%H:%M") if _trend["end"] is not None else ""}:' \
               f'{_trend["internal_low"]}-{_trend["internal_high"]}={_trend["internal_high"] - _trend["internal_low"]}'
        return text


class MultiTimeframeBasePattern(BasePattern, MultiTimeframe):
    pass
