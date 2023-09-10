import pandera
from pandas import Timestamp
from pandera import typing as pt


class BullBearSide(pandera.DataFrameModel):
    date: pt.Index[Timestamp]  # start
    bull_bear_side: pt.Series[str]
    end: pt.Series[Timestamp]
    internal_high: pt.Series[float]
    high_time: pt.Series[Timestamp]
    internal_low: pt.Series[float]
    low_time: pt.Series[Timestamp]
    movement: pt.Series[float]
    # strength: pt.Series[float]
    ATR: pt.Series[float]
    # duration: pt.Series[timedelta]
    movement_start_value: pt.Series[float] = pandera.Field(nullable=True)
    movement_end_value: pt.Series[float] = pandera.Field(nullable=True)
    movement_start_time: pt.Series[Timestamp] = pandera.Field(nullable=True)
    movement_end_time: pt.Series[Timestamp] = pandera.Field(nullable=True)



