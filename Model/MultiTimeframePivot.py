import pandera
from pandera import typing as pt

from Model.MultiTimeframe import MultiTimeframe
from Model.Pivot import Pivot


class MultiTimeframePivot(Pivot, MultiTimeframe):
    hit: pt.Series[int] = pandera.Field()
