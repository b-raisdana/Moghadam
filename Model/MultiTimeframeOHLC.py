from datetime import datetime

import pandera
from pandas import Timestamp
from pandera import typing as pt

from DataPreparation import MultiTimeframe


class OHLC(pandera.DataFrameModel):
    date: pt.Index[Timestamp]
    open: pt.Series[float]
    close: pt.Series[float]
    high: pt.Series[float]
    low: pt.Series[float]
    volume: pt.Series[float]


class MultiTimeframeOHLC(OHLC, MultiTimeframe):
    pass
