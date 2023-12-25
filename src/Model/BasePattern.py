from datetime import datetime
from typing import Annotated, Optional

import pandas as pd
import pandera
from pandera import typing as pt

from Model.MultiTimeframe import MultiTimeframe


class BasePattern(pandera.DataFrameModel):
    date: pt.Index[Annotated[pd.DatetimeTZDtype, "ns", "UTC"]]
    end: pt.Series[Annotated[pd.DatetimeTZDtype, "ns", "UTC"]] = pandera.Field(nullable=True)
    ttl: pt.Series[Annotated[pd.DatetimeTZDtype, "ns", "UTC"]]
    ATR: pt.Series[float]
    internal_high: pt.Series[float]
    internal_low: pt.Series[float]
    upper_band_activated: pt.Series[Annotated[pd.DatetimeTZDtype, "ns", "UTC"]] = pandera.Field(nullable=True)
    bellow_band_activated: pt.Series[Annotated[pd.DatetimeTZDtype, "ns", "UTC"]] = pandera.Field(nullable=True)

    @classmethod
    def repr(cls, _start: datetime, pattern):
        text = f'BASE: ' \
               f'{_start.strftime("%H:%M")}-{pattern["end"].strftime("%H:%M") if pattern["end"] is not None else ""}:' \
               f'{pattern["internal_low"]}-{pattern["internal_high"]}={pattern["internal_high"] - pattern["internal_low"]}'\
               f'ATR={pattern["ATR"]}'
        if hasattr(pattern, 'upper_band_activated') and pattern['upper_band_activated'] is not None:
            text += f"U@{pattern['upper_band_activated'].strftime('%H:%M')}"
        if hasattr(pattern, 'bellow_band_activated') and pattern['bellow_band_activated'] is not None:
            text += f"U@{pattern['bellow_band_activated'].strftime('%H:%M')}"
        return text


class MultiTimeframeBasePattern(BasePattern, MultiTimeframe):
    pass
