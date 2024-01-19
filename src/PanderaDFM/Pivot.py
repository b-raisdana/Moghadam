from datetime import datetime
from typing import Annotated

import pandas as pd
import pandera
from pandera import typing as pt

from PanderaDFM.MultiTimeframe import MultiTimeframe


class Pivot(pandera.DataFrameModel):
    date: pt.Index[Annotated[pd.DatetimeTZDtype, "ns", "UTC"]]
    level: pt.Series[float]
    is_resistance: pt.Series[bool]
    internal_margin: pt.Series[float]
    external_margin: pt.Series[float]
    activation_time: pt.Series[Annotated[pd.DatetimeTZDtype, "ns", "UTC"]]
    ttl: pt.Series[Annotated[pd.DatetimeTZDtype, "ns", "UTC"]]  # = pandera.Field(nullable=True)
    deactivated_at: pt.Series[Annotated[pd.DatetimeTZDtype, "ns", "UTC"]] = pandera.Field(nullable=True)
    archived_at: pt.Series[Annotated[pd.DatetimeTZDtype, "ns", "UTC"]] = pandera.Field(nullable=True)
    hit: pt.Series[int] = pandera.Field(nullable=True)
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
        if pd.notna(pivot_info['is_overlap_of']):
            output += f"O{pivot_info['is_overlap_of']}"
        return output

    @staticmethod
    def name(start_time: datetime, pivot_timeframe: str, pivot_info) -> str:
        output = f"Pivot {pivot_info['level']:.0f}={pivot_timeframe}@{start_time.strftime('%m/%d.%H:%M')}"
        if pd.notna(pivot_info['is_overlap_of']):
            output += f"O{pivot_info['is_overlap_of']}"
        return output


class MultiTimeframePivot(Pivot, MultiTimeframe):
    pass
    # hit: pt.Series[int] = pandera.Field()
