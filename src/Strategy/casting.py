# from __future__ import annotations
from datetime import datetime
from typing import Annotated

import numpy as np
import pandas as pd
import pandera
import pytz
from pandas import Timestamp
from pandera import typing as pt

import helper.data_preparation
from PanderaDFM.ExtendedDf import BasePanderaDFM
from PanderaDFM.SignalDf import SignalDFM

# class BasePattern(pandera.DataFrameModel):
#     date: pt.Index[Annotated[pd.DatetimeTZDtype, "ns", "UTC"]] = pandera.Field(check_name=True)
#     end: pt.Series[Annotated[pd.DatetimeTZDtype, "ns", "UTC"]] = pandera.Field(nullable=True)
#     ttl: pt.Series[Annotated[pd.DatetimeTZDtype, "ns", "UTC"]]
#     atr: pt.Series[float]
#     zero_trigger_candle: pt.Series[bool]
#     a_pattern_atr: pt.Series[float]
#     a_trigger_atr: pt.Series[float]
#     internal_high: pt.Series[float]
#     internal_low: pt.Series[float]
#     upper_band_activated: pt.Series[Annotated[pd.DatetimeTZDtype, "ns", "UTC"]] = pandera.Field(nullable=True)
#     below_band_activated: pt.Series[Annotated[pd.DatetimeTZDtype, "ns", "UTC"]] = pandera.Field(nullable=True)
#     size: pt.Series[float] = pandera.Field(nullable=True, default=np.NAN)
#     base_timeframe_atr: pt.Series[float] = pandera.Field(nullable=True, default=np.NAN)
#     ignore_backtesting: pt.Series[bool] = pandera.Field(nullable=True, default=np.NAN)
#
#
# class MultiTimeframeBasePattern(BasePattern, MultiTimeframe):
#     pass
#
# _sample_df = pt.DataFrame({
#     'date': [Timestamp(datetime(year=1980, month=1, day=1, hour=1, minute=1, second=1).replace(tzinfo=pytz.UTC))],
#     'timeframe': ['1min'],
#     'end': [np.NAN],
#     'ttl': [Timestamp(datetime(year=1980, month=1, day=1, hour=1, minute=1, second=1).replace(tzinfo=pytz.UTC))],
#     'atr': [0.0],
#     'zero_trigger_candle': [np.NAN],
#     'a_pattern_atr': [0.0],
#     'a_trigger_atr': [0.0],
#     'internal_high': [0.0],
#     'internal_low': [0.0],
#     'upper_band_activated': [np.NAN],
#     'below_band_activated': [np.NAN],
#     'size': [np.NAN],
#     'base_timeframe_atr': [np.NAN],
#     'ignore_backtesting': [np.NAN],
# })
# _sample_df = _sample_df.set_index(['timeframe', 'date'])  # , 'ref_date', 'ref_timeframe', 'side'])
# # helper.data_preparation.cast_and_validate(_sample_df, MultiTimeframeBasePattern)

# class SignalDFM(BasePanderaDFM):
#     # date: pt.Index[Annotated[pd.DatetimeTZDtype, "ns", "UTC"]]
#     # ref_date: pt.Index[Annotated[pd.DatetimeTZDtype, "ns", "UTC"]]
#     # ref_timeframe: pt.Index[str]
#     # side: pt.Index[str]
#     #
#     # original_index: pt.Series[Annotated[pd.DatetimeTZDtype, "ns", "UTC"]]
#     # end: pt.Series[Annotated[pd.DatetimeTZDtype, "ns", "UTC"]]
#     # base_asset_amount: pt.Series[float] = pandera.Field(nullable=True, default=None)
#
#     # @pandera.dataframe_check
#     # def end_after_start_check(cls, df, *args, **kwargs):
#     #     # Custom check to ensure 'end' is after 'date'
#     #     return df['end'] > df['date']
#
#     # from start of exact this candle the signal is valid
#     date: pt.Index[Annotated[pd.DatetimeTZDtype, "ns", "UTC"]]  # = pandera.Field(title='date')
#     ref_date: pt.Index[Annotated[pd.DatetimeTZDtype, "ns", "UTC"]] = pandera.Field(
#         title='ref_date')  # = pandera.Field(nullable=True)
#     ref_timeframe: pt.Index[str]  # = pandera.Field(title='ref_timeframe') # = pandera.Field(nullable=True)
#     side: pt.Index[str]  # = pandera.Field(default='side')  # sell or buy
#
#     original_index: pt.Series[Annotated[pd.DatetimeTZDtype, "ns", "UTC"]]
#     # from start of exact this candle the signal is in-valid
#     end: pt.Series[Annotated[pd.DatetimeTZDtype, "ns", "UTC"]]
#     base_asset_amount: pt.Series[float] = pandera.Field(nullable=True, default=None)
#     # the worst acceptable price for order execution.
#     limit_price: pt.Series[float] = pandera.Field(nullable=True, default=None)
#     stop_loss: pt.Series[float] = pandera.Field(nullable=True, default=None)  # , ignore_na=False
#     take_profit: pt.Series[float] = pandera.Field(nullable=True, default=None)
#     # the condition direction is reverse of limit direction.
#     trigger_price: pt.Series[float] = pandera.Field(nullable=True, default=None)
#     trigger_satisfied: pt.Series[bool] = pandera.Field(nullable=True, default=True)
#     original_order_id: pt.Series[str] = pandera.Field(nullable=True, default=None)
#     stop_loss_order_id: pt.Series[str] = pandera.Field(nullable=True, default=None)
#     take_profit_order_id: pt.Series[str] = pandera.Field(nullable=True, default=None)
#     order_is_active: pt.Series[bool] = pandera.Field(nullable=True, default=None)  # , ignore_na=False
#     # todo: if the signal end changed we have to update signal orders
#     updated: pt.Series[bool] = pandera.Field(nullable=True, default=True)


_sample_df = pt.DataFrame({
    'date': [Timestamp(datetime(year=1980, month=1, day=1, hour=1, minute=1, second=1).replace(tzinfo=pytz.UTC))],
    'ref_date': [
        Timestamp(datetime(year=1980, month=1, day=1, hour=1, minute=1, second=1).replace(tzinfo=pytz.UTC))],
    'ref_timeframe': ['1min'],
    'side': ['buy'],

    'original_index': [
        Timestamp(datetime(year=1980, month=1, day=1, hour=1, minute=1, second=1).replace(tzinfo=pytz.UTC))],
    'end': [Timestamp(datetime(year=1980, month=1, day=1, hour=1, minute=1, second=2).replace(tzinfo=pytz.UTC))],
    'base_asset_amount': [np.NAN],
})
_sample_df = _sample_df.set_index(
    ['date', 'ref_date', 'ref_timeframe', 'side'])  # , 'ref_date', 'ref_timeframe', 'side'])
_sample_df = helper.data_preparation.cast_and_validate2(_sample_df, SignalDFM)
SignalDFM.to_schema().validate(_sample_df)
pass
