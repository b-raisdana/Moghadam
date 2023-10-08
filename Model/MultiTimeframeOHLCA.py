import pandera
from pandera import typing as pt

from Model.MultiTimeframeOHLC import OHLCV, MultiTimeframe


class OHLCA(OHLCV):
    ATR: pt.Series[float] = pandera.Field(nullable=True)


class MultiTimeframeOHLCA(OHLCA, MultiTimeframe):
    pass
