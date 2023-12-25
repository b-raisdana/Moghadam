from typing import Annotated

import pandas as pd
import pandera
from pandera import typing as pt

from Model.MultiTimeframe import MultiTimeframe


class BasePattern(pandera.DataFrameModel):
    date: pt.Index[Annotated[pd.DatetimeTZDtype, "ns", "UTC"]]
    end: pt.Series[Annotated[pd.DatetimeTZDtype, "ns", "UTC"]]
    internal_high: pt.Series[float]
    internal_low: pt.Series[float]

class MultiTimeframeBasePattern(BasePattern, MultiTimeframe):
    pass