from datetime import datetime

import pandera
from pandas import Timestamp
from pandera import typing as pt

from DataPreparation import MultiTimeframe


class CandleTrend(pandera.DataFrameModel):
    date: pt.Index[datetime]
    open: pt.Series[float]
    close: pt.Series[float]
    low: pt.Series[float]
    high: pt.Series[float]
    volume: pt.Series[float]
    previous_peak_index: pt.Series[Timestamp] = pandera.Field(nullable=True)
    previous_peak_value: pt.Series[float] = pandera.Field(nullable=True)
    next_peak_index: pt.Series[Timestamp] = pandera.Field(nullable=True)
    next_peak_value: pt.Series[float] = pandera.Field(nullable=True)
    previous_valley_index: pt.Series[Timestamp] = pandera.Field(nullable=True)
    previous_valley_value: pt.Series[float] = pandera.Field(nullable=True)
    next_valley_index: pt.Series[Timestamp] = pandera.Field(nullable=True)
    next_valley_value: pt.Series[float] = pandera.Field(nullable=True)
    bull_bear_side: pt.Series[str]


class MultiTimeframeCandleTrend(CandleTrend, MultiTimeframe):
    pass
