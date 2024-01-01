import pandera
from pandera import typing as pt

from PanderaDFM.OHLCV import OHLCV
from PanderaDFM.MultiTimeframe import MultiTimeframe


class OHLCVA(OHLCV):
    ATR: pt.Series[float] = pandera.Field(nullable=True)


class MultiTimeframeOHLCVA(OHLCVA, MultiTimeframe):
    pass
