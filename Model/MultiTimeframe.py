import pandera
from pandera import typing as pt


class MultiTimeframe(pandera.DataFrameModel):
    timeframe: pt.Index[str]
