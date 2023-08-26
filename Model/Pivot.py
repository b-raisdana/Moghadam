from datetime import datetime

import pandera
from pandas import Timestamp
from pandera import typing as pt


class BasePivot(pandera.DataFrameModel):
    movement_start_time: pt.Series[Timestamp]
    movement_start_value: pt.Series[Timestamp]
    return_end_time: pt.Series[Timestamp]
    return_end_value: pt.Series[Timestamp]
    level: pt.Series[float]
    internal_margin: pt.Series[float]
    external_margin: pt.Series[float]
    activation_time: pt.Series[Timestamp]
    deactivation_time: pt.Series[Timestamp]
    hit: pt.Series[int]
    overlapped_with_major_timeframe: pt.Series[bool]


class Pivot(BasePivot):
    date: pt.Index[Timestamp]
