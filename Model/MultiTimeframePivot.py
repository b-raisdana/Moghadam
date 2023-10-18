import pandera
from pandera import typing as pt

from GPTnote import MultiTimeframe
from Model.Pivot import Pivot


class MultiTimeframePivot(Pivot, MultiTimeframe):
    hit: pt.Series[int] = pandera.Field()
