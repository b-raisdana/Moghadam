import pandera
from pandera import typing as pt

from DataPreparation import MultiTimeframe
from Model.MultiTimeframeOHLC import OHLCV


class OHLCA(OHLCV):
    ATR: pt.Series[float] = pandera.Field(nullable=True)


class MultiTimeframeOHLCA(OHLCA, MultiTimeframe):
    pass
