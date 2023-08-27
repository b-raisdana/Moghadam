from datetime import datetime

import pandera
from pandas import Timestamp
from pandera import typing as pt


class BasePivot(pandera.DataFrameModel):
    movement_start_time: pt.Series[Timestamp]
    movement_start_value: pt.Series[float]
    return_end_time: pt.Series[Timestamp]
    return_end_value: pt.Series[float]
    level: pt.Series[float]
    internal_margin: pt.Series[float]
    external_margin: pt.Series[float]
    activation_time: pt.Series[Timestamp]
    deactivation_time: pt.Series[Timestamp] = pandera.Field(nullable=True)
    hit: pt.Series[float] = pandera.Field(nullable=True, coerce=True)
    overlapped_with_major_timeframe: pt.Series[str] = pandera.Field(nullable=True)

    @staticmethod
    def repr(start_time: datetime, pivot_timeframe: str, pivot_info) -> str:
        output = (f"Pivot {pivot_info['level']}={pivot_timeframe}@{start_time}"
                  f"[{pivot_info['internal_margin']}-{pivot_info['external_margin']}]("
                  f"M{abs(pivot_info['movement_start_value'] - pivot_info['level'])}"
                  f"R{abs(pivot_info['return_end_value'] - pivot_info['level'])}"
                  f"H{pivot_info['hit']}")
        if len(pivot_info['overlapped_with_major_timeframe'] or '') > 0:
            output += f"O{pivot_info['overlapped_with_major_timeframe']}"
        return output


class Pivot(BasePivot):
    date: pt.Index[Timestamp]
