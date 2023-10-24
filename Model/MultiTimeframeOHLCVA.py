import pandera
from pandera import typing as pt

from Model.MultiTimeframeOHLCV import OHLCV
from Model.MultiTimeframe import MultiTimeframe


class OHLCVA(OHLCV):
    ATR: pt.Series[float] = pandera.Field(nullable=True)


class MultiTimeframeOHLCVA(OHLCVA, MultiTimeframe):
    pass
