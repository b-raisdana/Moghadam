from datetime import datetime
from typing import Annotated

import pandas as pd
import pandera
from pandas import Timestamp
from pandera import typing as pt

from PanderaDFM.ExtendedDf import ExtendedDf
from PanderaDFM.MultiTimeframe import MultiTimeframe


class PivotDFM(pandera.DataFrameModel):
    date: pt.Index[Annotated[pd.DatetimeTZDtype, "ns", "UTC"]]  # the original time of creating pivot
    level: pt.Series[float]
    is_resistance: pt.Series[bool]
    internal_margin: pt.Series[float]
    external_margin: pt.Series[float]
    original_start: pt.Series[Annotated[pd.DatetimeTZDtype, "ns", "UTC"]]  # this part activated at (passing time)
    ttl: pt.Series[Annotated[pd.DatetimeTZDtype, "ns", "UTC"]]  # = pandera.Field(nullable=True)
    passing: pt.Series[Annotated[pd.DatetimeTZDtype, "ns", "UTC"]] = pandera.Field(nullable=True)
    deactivated_at: pt.Series[Annotated[pd.DatetimeTZDtype, "ns", "UTC"]] = pandera.Field(nullable=True)
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


class MultiTimeframePivotDFM(PivotDFM, MultiTimeframe):
    pass


class MultiTimeframePivotDf(ExtendedDf):
    schema_data_frame_model = MultiTimeframePivotDFM
    _sample_df = None
    _empty_obj = None


_sample_df = pd.DataFrame({
    'date': [Timestamp(datetime(year=1980, month=1, day=1, hour=1, minute=1, second=1).replace(tzinfo=pytz.UTC))],
    'timeframe': ['test'],
    'level': [0.0],
    'is_resistance': [False],
    'internal_margin': [0.0],
    'original_start': [
        Timestamp(datetime(year=1980, month=1, day=1, hour=1, minute=1, second=1).replace(tzinfo=pytz.UTC))],
    'ttl': [Timestamp(datetime(year=1980, month=1, day=1, hour=1, minute=1, second=1).replace(tzinfo=pytz.UTC))],
})
MultiTimeframePivotDf._sample_df = _sample_df.set_index(['date', 'timeframe'])
