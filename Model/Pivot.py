from datetime import datetime

import pandera
from pandas import Timestamp
from pandera import typing as pt


class Pivot(pandera.DataFrameModel):
    date: pt.Index[Timestamp]

    level: pt.Series[float]
    internal_margin: pt.Series[float]
    external_margin: pt.Series[float]
    activation_time: pt.Series[Timestamp]
    ttl: pt.Series[Timestamp]  # = pandera.Field(nullable=True)
    deactivated_at: pt.Series[Timestamp] = pandera.Field(nullable=True)
    archived_at: pt.Series[Timestamp] = pandera.Field(nullable=True)
    hit: pt.Series[int] = pandera.Field(nullable=True, coerce=True)
    is_overlap_of: pt.Series[str] = pandera.Field(nullable=True)


    @staticmethod
    def description(start_time: datetime, pivot_timeframe: str, pivot_info) -> str:
        output = (f"Pivot {pivot_info['level']:.0f}={pivot_timeframe}@{start_time.strftime('%m/%d.%H:%M')}"
                  f"[{pivot_info['internal_margin']:.0f}-{pivot_info['external_margin']:.0f}](")

        if hasattr(pivot_info, 'movement_start_value'):
            output += f"M{abs(pivot_info['movement_start_value'] - pivot_info['level']):.0f}"
        if hasattr(pivot_info, 'return_end_value'):
            output += f"R{abs(pivot_info['return_end_value'] - pivot_info['level']):.0f}"
        output += f"H{pivot_info['hit']}"
        if len(pivot_info['is_overlap_of'] or '') > 0:
            output += f"O{pivot_info['is_overlap_of']}"
        return output


    @staticmethod
    def name(start_time: datetime, pivot_timeframe: str, pivot_info) -> str:
        output = f"Pivot {pivot_info['level']:.0f}={pivot_timeframe}@{start_time.strftime('%m/%d.%H:%M')}"
        if len(pivot_info['is_overlap_of'] or '') > 0:
            output += f"O{pivot_info['is_overlap_of']}"
        return output

class BullBearSidePivot(Pivot):
    movement_start_time: pt.Series[Timestamp] = pandera.Field(nullable=True)
    movement_start_value: pt.Series[float] = pandera.Field(nullable=True)
    return_end_time: pt.Series[Timestamp] = pandera.Field(nullable=True)
    return_end_value: pt.Series[float] = pandera.Field(nullable=True)
