'''
this files used as note for
change following code to every time we run this:
1. find gaps which we do not have OHLCV data for last 1 year period up to today morning 00:00:00.
2. fetch all the
'''
import numpy as np

'''
write the code to do:
    
def multi_timframe_strength(peaks_or_valleys: pt.DataFrame[PeaksValleys], mode: TopTYPE, ohlcv: pd.DataFrame[OHLCV]):
    prev_index_col = f'previous_{top_type.value}_index'
    prev_value_col = f'previous_{top_type.value}_value'
    next_index_col = f'next_{top_type.value}_index'
    next_value_col = f'next_{top_type.value}_value'
    
    if prev_index_col not in ohlcv.columns or next_index_col not in ohlcv.columns :
        ohlcv = insert_previous_n_next_top(top_type, peaks_n_valleys, ohlcv)
        
    if top_type == TopTYPE.PEAK:
        compare_column = 'high'
        compare_op = lambda x, y: x > y
    else:
        compare_column = 'low'
        compare_op = lambda x, y: x < y
    
    # Use the determined column and operation for comparison
    condition_previous = (ohlcv.index < peaks_or_valleys.index) & compare_op(ohlcv[compare_column], peaks_or_valleys[compare_column])
    condition_next = (ohlcv.index > peaks_or_valleys.index) & compare_op(ohlcv[compare_column], peaks_or_valleys[compare_column])
    
    peaks_or_valleys['nearest_previous_significant_ohlc'] = ohlcv[condition_previous].index.max()
    peaks_or_valleys['nearest_next_significant_ohlc'] = ohlcv[condition_next].index.min()

    peaks_or_valleys['left_distance'] = peaks_or_valleys.index - peaks_or_valleys['nearest_previous_significant_ohlc'] 
    peaks_or_valleys['right_distance'] = peaks_or_valleys['nearest_next_significant_ohlc'] - peaks_or_valleys.index
'''
import pandera
from pandas import Timestamp
from pandera import typing as pt
from enum import Enum
from datetime import timedelta, datetime
import pandas as pd


class OHLCV(pandera.DataFrameModel):
    date: pt.Index[Timestamp]
    open: pt.Series[float]
    close: pt.Series[float]
    high: pt.Series[float]
    low: pt.Series[float]
    volume: pt.Series[float]


class PeaksValleys(OHLCV):
    peak_or_valley: pandera.typing.Series[str]
    strength: pandera.typing.Series[float]  # pt.Timedelta  # pandera.typing.Series[np.timedelta64]


def insert_previous_n_next_top(top_type, peaks_n_valleys, ohlcv):
    raise Exception('not tested')
    # Define columns
    prev_index_col = f'previous_{top_type.value}_index'
    prev_value_col = f'previous_{top_type.value}_value'
    next_index_col = f'next_{top_type.value}_index'
    next_value_col = f'next_{top_type.value}_value'

    # Ensure columns exist
    for col in [prev_index_col, prev_value_col, next_index_col, next_value_col]:
        if col not in ohlcv.columns:
            ohlcv[col] = None

    # Filter the relevant tops
    tops = peaks_n_valleys[peaks_n_valleys['peak_or_valley'] == top_type.value].copy()

    high_or_low = 'high' if top_type == TopTYPE.PEAK else 'low'

    # Using `shift()` to create the previous and next columns
    tops[prev_index_col] = tops.index.to_series().shift(-1)
    tops[prev_value_col] = tops[high_or_low].shift(-1)
    tops[next_index_col] = tops.index.to_series().shift(1)
    tops[next_value_col] = tops[high_or_low].shift(1)

    # Using `merge_asof` to efficiently merge the previous and next values
    ohlcv = pd.merge_asof(ohlcv.sort_index(), tops[[prev_index_col, prev_value_col]],
                          left_index=True, right_index=True, direction='forward', suffixes=('', '_y'))
    ohlcv = pd.merge_asof(ohlcv.sort_index(), tops[[next_index_col, next_value_col]],
                          left_index=True, right_index=True, direction='backward', suffixes=('', '_y'))

    # Cleaning any duplicate columns
    for col in ohlcv.columns:
        if col.endswith('_y'):
            ohlcv.drop(col, axis=1, inplace=True)

    return ohlcv


class TopTYPE(Enum):
    PEAK = 'peak'
    VALLEY = 'valley'


INFINITY_TIME_DELTA = timedelta(days=10 * 365)


def calculate_strength_vectorized(peaks_or_valleys: pt.DataFrame[PeaksValleys], top_type: TopTYPE,
                                  ohlcv: pt.DataFrame[OHLCV]):
    # todo: test calculate_strength
    if len(peaks_or_valleys) == 0:
        return peaks_or_valleys
    start_time_of_prices = ohlcv.index[0]
    peaks_or_valleys['strength'] = INFINITY_TIME_DELTA

    if top_type == TopTYPE.PEAK:
        compare_column = 'high'

        def compare_op(x, y):
            return x > y
    else:
        compare_column = 'low'

        def compare_op(x, y):
            return x < y

    # ohlcv_top_indexes = ohlcv.index.isin(peaks_or_valleys.index)
    ohlcv_top_indexes = peaks_or_valleys.index

    # I want to add a column to ohlcv like next_crossing_ohlcv which represent index of nearest ohlcv row which it's
    # high value is greater than high value of the row.

    # Create a boolean mask for values where the 'high' column is greater than its previous value.
    next_higher_mask = compare_op(ohlcv[compare_column].shift(-1), ohlcv[compare_column])
    ohlcv['next_crossing_ohlcv'] = np.where(next_higher_mask, ohlcv.index.shift(-1), np.nan).bfill()

    previous_higher_mask = compare_op(ohlcv[compare_column].shift(1), ohlcv[compare_column])
    ohlcv['previous_crossing_ohlcv'] = np.where(previous_higher_mask, ohlcv.index.shift(1), np.nan).ffill()
    peaks_or_valleys['left_distance'] = peaks_or_valleys.index - ohlcv.loc[ohlcv_top_indexes, 'previous_crossing_ohlcv']
    left_na_indexes = peaks_or_valleys[pd.isna(peaks_or_valleys['left_distance'])].index
    peaks_or_valleys.loc[left_na_indexes, 'left_distance'] = left_na_indexes - start_time_of_prices
    # peaks_or_valleys.loc[peaks_or_valleys.index[0], 'left_distance'] = peaks_or_valleys.index[0] - start_time_of_prices
    peaks_or_valleys['right_distance'] = ohlcv.loc[ohlcv_top_indexes, 'next_crossing_ohlcv'] - peaks_or_valleys.index
    right_na_indexes = peaks_or_valleys[pd.isna(peaks_or_valleys['right_distance'])].index
    peaks_or_valleys.loc[right_na_indexes, 'right_distance'] = INFINITY_TIME_DELTA
    # peaks_or_valleys.loc[peaks_or_valleys.index[-1], 'right_distance'] = INFINITY_TIME_DELTA
    peaks_or_valleys['strength'] = peaks_or_valleys[['left_distance', 'right_distance']].min(axis=1)

    peaks_or_valleys['strength'] = peaks_or_valleys['strength'].dt.total_seconds()
    return peaks_or_valleys


def calculate_strength_by_apply(peaks_or_valleys: pd.DataFrame, top_type: TopTYPE, ohlcv: pd.DataFrame):
    # todo: test calculate_strength
    start_time_of_prices = ohlcv.index[0]

    # if prev_index_col not in ohlcv.columns or next_index_col not in ohlcv.columns:
    #     ohlcv = insert_previous_n_next_top(top_type, peaks_n_valleys, ohlcv)
    peaks_or_valleys['strength'] = INFINITY_TIME_DELTA

    if top_type == TopTYPE.PEAK:
        compare_column = 'high'

        def compare_op(x, y):
            return x > y
    else:
        compare_column = 'low'

        def compare_op(x, y):
            return x < y

    def right_distance(row_index: datetime) -> timedelta:
        _index = ohlcv.index[
            (ohlcv.index > row_index) &
            compare_op(ohlcv[compare_column], peaks_or_valleys.loc[row_index, compare_column])].min()
        if _index is not None:
            return _index - row_index
        else:
            return INFINITY_TIME_DELTA

    peaks_or_valleys['right_distance'] = peaks_or_valleys.index.to_series().apply(right_distance)

    def left_distance(row_index: datetime) -> timedelta:
        _index = ohlcv.index[
            (ohlcv.index < row_index) &
            compare_op(ohlcv[compare_column], peaks_or_valleys.loc[row_index, compare_column])].max()
        if _index is not None:
            return row_index - _index
        else:
            return row_index - start_time_of_prices

    peaks_or_valleys['left_distance'] = peaks_or_valleys.index.to_series().apply(left_distance)

    peaks_or_valleys['strength'] = peaks_or_valleys[['left_distance', 'right_distance']].min(axis=1)

    peaks_or_valleys['strength'] = peaks_or_valleys['strength'].dt.total_seconds()
    return peaks_or_valleys
