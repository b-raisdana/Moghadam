import os
from datetime import timedelta
from typing import Tuple, List, Optional

import pandas as pd
import pandera.typing as pt
from pandas import Timestamp

from Config import TopTYPE, config, TREND
from Model.BullBearSide import MultiTimeframeBullBearSide, BullBearSide, bull_bear_side_repr
from Model.CandleTrend import MultiTimeframeCandleTrend
from Model.OHLCV import OHLCV
from Model.OHLCVA import OHLCVA
from Model.PeakValley import PeakValley, MultiTimeframePeakValley
from PeakValley import peaks_only, valleys_only, read_multi_timeframe_peaks_n_valleys, major_peaks_n_valleys, \
    insert_previous_n_next_top
from atr import read_multi_timeframe_ohlcva
from data_preparation import read_file, single_timeframe, to_timeframe, cast_and_validate, empty_df, concat, \
    date_range_of_data
from helper import log, measure_time, LogSeverity
from ohlcv import read_multi_timeframe_ohlcv


@measure_time
def insert_previous_n_next_tops(single_timeframe_peaks_n_valleys: pt.DataFrame[PeakValley], ohlcv: pt.DataFrame[OHLCV]) \
        -> pt.DataFrame[OHLCV]:
    log('********************************01', stack_trace=False)
    ohlcv = insert_previous_n_next_top(TopTYPE.PEAK, single_timeframe_peaks_n_valleys, ohlcv)
    log('********************************02', stack_trace=False)
    ohlcv = insert_previous_n_next_top(TopTYPE.VALLEY, single_timeframe_peaks_n_valleys, ohlcv)
    log('********************************03', stack_trace=False)
    return ohlcv


def single_timeframe_candles_trend(ohlcv: pt.DataFrame[OHLCV],
                                   single_timeframe_peaks_n_valley: pt.DataFrame[PeakValley]) -> pd.DataFrame:
    # Todo: Not tested!
    candle_trend = insert_previous_n_next_tops(single_timeframe_peaks_n_valley, ohlcv)
    log("1", stack_trace=False)
    candle_trend['bull_bear_side'] = TREND.SIDE.value
    higher_peaks_and_valleys = candle_trend[
        ((candle_trend['next_peak_value'] > candle_trend['previous_peak_value'])
         & (candle_trend['next_valley_value'] > candle_trend['previous_valley_value']))
    ].index
    log("2", stack_trace=False)
    if len(higher_peaks_and_valleys) > 0:
        candle_trend.loc[
            higher_peaks_and_valleys,  # todo: the higher peak should be after higher valley
            'bull_bear_side'] = TREND.BULLISH.value
        log("3", stack_trace=False)
    lower_peaks_and_valleys = candle_trend[
        ((candle_trend['next_peak_value'] < candle_trend['previous_peak_value'])
         & (candle_trend['next_valley_value'] < candle_trend['previous_valley_value']))
    ].index
    if len(lower_peaks_and_valleys) > 0:
        candle_trend.loc[
            lower_peaks_and_valleys,  # todo: the lower valley should be after lower peak
            'bull_bear_side'] = TREND.BEARISH.value
        log(f"4 {len(single_timeframe_peaks_n_valley)}", stack_trace=False)
    log(f"5", stack_trace=False)
    return candle_trend


# def add_previous_toward_trend_top_to_boundary(single_timeframe_boundaries: pt.DataFrame[BullBearSide],
#                                               single_timeframe_peaks_n_valleys: pt.DataFrame[PeaksValleys],
#                                               timeframe: str) -> pt.DataFrame:
#     """
#     if trend is not side
#         if trend is bullish
#             find next_peak (first peak after the boundary finishes) and last_peak inside of boundary
#             if  next_peak['high'] > last_peak
#                 expand boundary to include next_peak
#         else: # if trend is bullish
#             find next_valley (first valley after the boundary finishes) and last_valley inside of boundary
#             if  next_peak['high'] > last_peak
#                 expand boundary to include next_valley
#     """
#     if not single_timeframe_boundaries.index.is_unique:
#         raise Exception('We expect the single_timeframe_boundaries index be unique but isn\'t. '
#                         'So we may have unexpected results after renaming DataFrame index to move boundary start.')
#     single_timeframe_peaks = peaks_only(single_timeframe_peaks_n_valleys)
#     single_timeframe_valleys = valleys_only(single_timeframe_peaks_n_valleys)
#     boundary_index: Timestamp
#     for boundary_index, boundary in single_timeframe_boundaries.iterrows():
#         # if boundary_index == Timestamp('2017-10-06 02:15:00'):
#         #     pass
#         if boundary.bull_bear_side == TREND.BULLISH.value:
#             last_valley_before_boundary_time, last_valley_before_boundary = \
#                 previous_top_of_boundary(boundary_index, single_timeframe_valleys)
#             if last_valley_before_boundary_time is not None:
#                 if last_valley_before_boundary['low'] < boundary['internal_low']:
#                     last_valley_before_boundary_time_mapped = to_timeframe(last_valley_before_boundary_time, timeframe)
#                     single_timeframe_boundaries \
#                         .rename(index={boundary_index: last_valley_before_boundary_time_mapped},
#                                 inplace=True)
#                     single_timeframe_boundaries.loc[last_valley_before_boundary_time_mapped, 'internal_low'] = \
#                         last_valley_before_boundary['low']
#         elif boundary.bull_bear_side == TREND.BEARISH.value:
#             last_peak_before_boundary_time, last_peak_before_boundary = \
#                 previous_top_of_boundary(boundary_index, single_timeframe_peaks)
#             if last_peak_before_boundary_time is not None:
#                 if last_peak_before_boundary['high'] > boundary['internal_high']:
#                     last_peak_before_boundary_time_mapped = to_timeframe(last_peak_before_boundary_time, timeframe)
#                     single_timeframe_boundaries \
#                         .rename(index={boundary_index: last_peak_before_boundary_time_mapped},
#                                 inplace=True)
#                     single_timeframe_boundaries.loc[last_peak_before_boundary_time_mapped, 'internal_high'] = \
#                         last_peak_before_boundary['high']
#     return single_timeframe_boundaries

def expand_trend_by_near_tops(timeframe_boundaries: pt.DataFrame[BullBearSide],
                              timeframe_peaks: pt.DataFrame[MultiTimeframePeakValley],
                              timeframe_valleys: pt.DataFrame[MultiTimeframePeakValley], trend: TREND):
    # timeframe_boundaries['movement_end_time'] = timeframe_boundaries['end']
    # timeframe_boundaries['movement_start_time'] = timeframe_boundaries.index
    # timeframe_boundaries['movement_end_value'] = pd.Series(dtype=float)
    # timeframe_boundaries['movement_start_value'] = pd.Series(dtype=float)
    if len(timeframe_boundaries) == 0:
        return timeframe_boundaries
    if trend == TREND.BULLISH:
        end_significant_column = 'high'
        end_tops = timeframe_peaks
        start_significant_column = 'low'
        start_tops = timeframe_valleys

        # timeframe_boundaries['movement_start_value'] = timeframe_boundaries['internal_low']
        # timeframe_boundaries['movement_end_value'] = timeframe_boundaries['internal_high']

        def more_significant_end(x: float, y: float):
            return x > y

        def more_significant_start(x: float, y: float):
            return x < y
    elif trend == TREND.BEARISH:
        end_significant_column = 'low'
        end_tops = timeframe_valleys
        start_significant_column = 'high'
        start_tops = timeframe_peaks

        # timeframe_boundaries['movement_start_value'] = timeframe_boundaries['internal_high']
        # timeframe_boundaries['movement_end_value'] = timeframe_boundaries['internal_low']

        def more_significant_end(x: float, y: float):
            return x < y

        def more_significant_start(x: float, y: float):
            return x > y
    else:
        raise Exception(f"Invalid boundary['bull_bear_side']={trend}")
    shifted_next_tops = shifted_time_and_value(end_significant_column, end_tops)

    previous_top_indexes = start_tops.index.shift(1, freq=config.timeframes[0])
    shifted_previous_tops = pd.DataFrame(index=previous_top_indexes).sort_index()
    shifted_previous_tops['previous_top_time'] = start_tops.index.tolist()
    shifted_previous_tops['previous_top_value'] = start_tops[start_significant_column].tolist()

    previous_round_movement_end_time = pd.Series()
    previous_round_movement_start_time = pd.Series()
    # todo: test this loop
    while (
            # (len(previous_round_movement_end_time) != len(timeframe_boundaries['movement_end_time']))
            # or
            # (len(previous_round_movement_start_time) != len(timeframe_boundaries['movement_start_time']))
            # or
            (not previous_round_movement_end_time.equals(timeframe_boundaries['movement_end_time']))
            or
            (not previous_round_movement_start_time.equals(timeframe_boundaries['movement_start_time']))
    ):
        previous_round_movement_end_time = timeframe_boundaries['movement_end_time']
        previous_round_movement_start_time = timeframe_boundaries['movement_start_time']
        timeframe_boundaries.drop(
            columns=['next_top_value', 'next_top_time', 'previous_top_time', 'previous_top_value'],
            inplace=True, errors='ignore')
        timeframe_boundaries = pd.merge_asof(timeframe_boundaries.sort_values(by='movement_end_time'),
                                             shifted_next_tops, direction='forward',
                                             left_on='movement_end_time', right_index=True)
        if 'next_top_value' not in timeframe_boundaries.columns:
            pass
        end_expandable_indexes = timeframe_boundaries[more_significant_end(
            timeframe_boundaries[f'next_top_value'], timeframe_boundaries[f'internal_{end_significant_column}'])].index
        timeframe_boundaries.loc[end_expandable_indexes, 'movement_end_time'] = \
            timeframe_boundaries.loc[end_expandable_indexes, 'next_top_time']
        if 'next_top_value' not in timeframe_boundaries.columns:
            pass
        timeframe_boundaries.loc[end_expandable_indexes, 'movement_end_value'] = \
            timeframe_boundaries.loc[end_expandable_indexes, 'next_top_value']
        timeframe_boundaries.loc[end_expandable_indexes, f'internal_{end_significant_column}'] = \
            timeframe_boundaries.loc[end_expandable_indexes, 'next_top_value']

        if not timeframe_boundaries.index.is_monotonic_increasing:
            pass
        timeframe_boundaries = pd.merge_asof(timeframe_boundaries.sort_index(),
                                             shifted_previous_tops, direction='backward',
                                             left_index=True, right_index=True)
        start_expandable_indexes = timeframe_boundaries[more_significant_start(
            timeframe_boundaries[f'previous_top_value'],
            timeframe_boundaries[f'internal_{start_significant_column}'])].index
        timeframe_boundaries.loc[start_expandable_indexes, 'movement_start_time'] = \
            timeframe_boundaries.loc[start_expandable_indexes, 'previous_top_time']
        timeframe_boundaries.loc[start_expandable_indexes, 'movement_start_value'] = \
            timeframe_boundaries.loc[start_expandable_indexes, 'previous_top_value']
        timeframe_boundaries.loc[end_expandable_indexes, f'internal_{start_significant_column}'] = \
            timeframe_boundaries.loc[end_expandable_indexes, 'previous_top_value']
    return timeframe_boundaries


def shifted_time_and_value(end_significant_column, end_tops):
    next_top_indexes = end_tops.index.shift(-1, freq=config.timeframes[0])
    shifted_next_tops = pd.DataFrame(index=next_top_indexes).sort_index()
    shifted_next_tops['next_top_time'] = end_tops.index.tolist()
    shifted_next_tops['next_top_value'] = end_tops[end_significant_column].tolist()
    return shifted_next_tops


def add_movement_range(timeframe_bbs: pt.DataFrame[BullBearSide],
                       timeframe_peaks_n_valleys: pd.DataFrame,
                       trend: TREND
                       ) -> pd.DataFrame:
    assert 'movement_start_time' not in timeframe_bbs.columns
    # _timeframe_bull_bear_side_trends['movement_start_time'] = pd.Series(dtype='datetime64[ns, UTC]')
    assert 'movement_end_time' not in timeframe_bbs.columns
    # _timeframe_bull_bear_side_trends['movement_end_time'] = pd.Series(dtype='datetime64[ns, UTC]')
    assert 'movement_start_value' not in timeframe_bbs.columns
    # _timeframe_bull_bear_side_trends['movement_start_value'] = p.Series(dtype=float)
    assert 'movement_end_value' not in timeframe_bbs.columns
    # _timeframe_bull_bear_side_trends['movement_end_value'] = pd.Series(dtype=float)
    timeframe_bbs['movement_end_time'] = timeframe_bbs['end']
    timeframe_bbs['movement_start_time'] = timeframe_bbs.index
    timeframe_bbs['movement_end_value'] = pd.Series(dtype=float)
    timeframe_bbs['movement_start_value'] = pd.Series(dtype=float)
    if len(timeframe_bbs) == 0:
        return timeframe_bbs
    if trend == TREND.BULLISH:
        movement_end_column = 'high'
        movement_end_tops = timeframe_peaks
        movement_start_column = 'low'
        movement_start_tops = timeframe_valleys

        timeframe_bbs['movement_start_value'] = timeframe_bbs['internal_low']
        timeframe_bbs['movement_end_value'] = timeframe_bbs['internal_high']

        def more_significant_end(x: float, y: float):
            return x > y

        def more_significant_start(x: float, y: float):
            return x < y
    elif trend == TREND.BEARISH:
        movement_end_column = 'low'
        movement_end_tops = timeframe_valleys
        movement_start_column = 'high'
        movement_start_tops = timeframe_peaks

        timeframe_bbs['movement_start_value'] = timeframe_bbs['internal_high']
        timeframe_bbs['movement_end_value'] = timeframe_bbs['internal_low']

        def more_significant_end(x: float, y: float):
            return x < y

        def more_significant_start(x: float, y: float):
            return x > y
    else:
        raise Exception(f"Invalid boundary['bull_bear_side']={trend}")


@measure_time
def expand_trends_by_near_tops(_timeframe_bull_bear_side_trends: pt.DataFrame[BullBearSide],
                               _timeframe_peaks_n_valleys: pd.DataFrame,
                               ) -> pd.DataFrame:
    """
    Expand boundaries towards the previous trend top.

    This function expands boundaries that are not of 'SIDE' trend towards the previous top of the same trend
    (either bullish or bearish) inside the boundary. If the trend is bullish, it finds the next peak (first peak
    after the boundary finishes) and the last peak inside the boundary. If the next peak's high value is greater
    than the last peak, the boundary is expanded to include the next peak. Similarly, for a bearish trend, it finds
    the next valley (first valley after the boundary finishes) and the last valley inside the boundary. If the
    next valley's low value is lower than the last valley, the boundary is expanded to include the next valley.

    Parameters:
        _timeframe_bull_bear_side_trends (pd.DataFrame): DataFrame containing boundaries for the specified timeframe.
        _timeframe_peaks_n_valleys (pd.DataFrame): DataFrame containing peaks and valleys for the same timeframe.
        timeframe (str): The timeframe for which the boundaries and peaks/valleys are calculated.

    Returns:
        pd.DataFrame: The input DataFrame with added columns 'movement_start_value' and 'movement_end_value'.

    Raises:
        Exception: If the DataFrame index of single_timeframe_boundaries is not unique.

    Example:
        # Expand boundaries towards the previous trend top for a specific timeframe
        updated_boundaries = add_toward_top_to_trend(single_timeframe_boundaries,
                                                     single_timeframe_peaks_n_valleys,
                                                     '15min')
        print(updated_boundaries)
    """
    assert 'movement_start_time' in _timeframe_bull_bear_side_trends.columns
    # _timeframe_bull_bear_side_trends['movement_start_time'] = pd.Series(dtype='datetime64[ns, UTC]')
    assert 'movement_end_time' in _timeframe_bull_bear_side_trends.columns
    # _timeframe_bull_bear_side_trends['movement_end_time'] = pd.Series(dtype='datetime64[ns, UTC]')
    assert 'movement_start_value' in _timeframe_bull_bear_side_trends.columns
    # _timeframe_bull_bear_side_trends['movement_start_value'] = pd.Series(dtype=float)
    assert 'movement_end_value' in _timeframe_bull_bear_side_trends.columns
    # _timeframe_bull_bear_side_trends['movement_end_value'] = pd.Series(dtype=float)
    if len(_timeframe_bull_bear_side_trends) > 0:
        if not _timeframe_bull_bear_side_trends.index.is_unique:
            raise Exception('We expect the single_timeframe_boundaries index be unique but isn\'t. '
                            'So we may have unexpected results after renaming DataFrame index to move boundary start.')
        timeframe_peaks = peaks_only(_timeframe_peaks_n_valleys).sort_index(level='date')
        timeframe_valleys = valleys_only(_timeframe_peaks_n_valleys).sort_index(level='date')

        bullish_indexes = _timeframe_bull_bear_side_trends[
            _timeframe_bull_bear_side_trends['bull_bear_side'] == TREND.BULLISH.value
            ].index
        bearish_indexes = _timeframe_bull_bear_side_trends[
            _timeframe_bull_bear_side_trends['bull_bear_side'] == TREND.BEARISH.value
            ].index
        if len(bullish_indexes) > 0:
            _timeframe_bull_bear_side_trends.loc[bullish_indexes] = \
                expand_trend_by_near_tops(_timeframe_bull_bear_side_trends.loc[bullish_indexes],
                                          timeframe_peaks, timeframe_valleys, trend=TREND.BULLISH)
        if len(bearish_indexes) > 0:
            _timeframe_bull_bear_side_trends.loc[bearish_indexes] = \
                expand_trend_by_near_tops(_timeframe_bull_bear_side_trends.loc[bearish_indexes],
                                          timeframe_peaks, timeframe_valleys, trend=TREND.BEARISH)
    assert not _timeframe_bull_bear_side_trends.isna().all(axis=1).any()
    return _timeframe_bull_bear_side_trends


# @measure_time
# def bull_bear_boundary_movement(boundary: pd.Series, start: datetime, single_timeframe_peaks: pd.DataFrame,
#                                 single_timeframe_valleys: pd.DataFrame) \
#         -> Tuple[float, float, pd.Timestamp, pd.Timestamp]:
#     """
#     Calculate the movement range for a bullish or bearish boundary.
#
#     This function calculates the start and end points of the movement range for a given bullish or bearish boundary.
#     The start point is determined based on the previous valley before the boundary, and the end point is determined
#     based on the next peak after the boundary.
#
#     Parameters:
#         boundary (pd.Series): The boundary for which to calculate the movement range.
#         start (pd.Timestamp): The timestamp index of the boundary.
#         single_timeframe_peaks (pd.DataFrame): DataFrame containing peaks for the same timeframe as the boundary.
#         single_timeframe_valleys (pd.DataFrame): DataFrame containing valleys for the same timeframe as the boundary.
#
#     Returns:
#         Tuple[float, float, pd.Timestamp, pd.Timestamp]: A tuple containing the start value, end value,
#                                                         start time, and end time of the movement range.
#
#     Raises:
#         Exception: If the 'bull_bear_side' value in the boundary is not valid.
#
#     Example:
#         # Calculate the movement range for a bullish boundary
#         boundary_series = single_timeframe_peaks.loc[boundary_index]
#         start_val, end_val, start_time, end_time = bull_bear_boundary_movement(boundary_series, boundary_index,
#                                                                                single_timeframe_peaks,
#                                                                                single_timeframe_valleys)
#         print(f"Start of movement value: {start_val}, End of movement value: {end_val}")
#         print(f"Start of movement time: {start_time}, End of movement time: {end_time}")
#     """
#     if boundary['bull_bear_side'] == TREND.BULLISH.value:
#         tops = single_timeframe_valleys
#         movement_end_column = 'high'
#         movement_start_column = 'low'
#     elif boundary['bull_bear_side'] == TREND.BEARISH.value:
#         tops = single_timeframe_peaks
#         movement_end_column = 'low'
#         movement_start_column = 'high'
#     else:
#         raise Exception(f"Invalid boundary['bull_bear_side']={boundary['bull_bear_side']}")
#     next_top_times = tops.index.shift(1, freq=config.timeframes[0]).tolist()
#     previous_top_times = tops.index.shift(-1, freq=config.timeframes[0]).tolist()
#     shifted_top_times = list(set(next_top_times + previous_top_times))  # merge and remove duplicate time indexes
#     shifted_tops = pd.DataFrame(index=shifted_top_times)
#     shifted_tops.loc[next_top_times, 'next_top_time'] = tops.index
#     shifted_tops.loc[previous_top_times, 'previous_top_time'] = tops.index
#
#     if boundary['bull_bear_side'] == TREND.BULLISH.value:
#         time_of_last_top_before_boundary, value_of_last_top_before_boundary = \
#             boundary_previous_top(start, boundary, single_timeframe_valleys, TREND.BULLISH)
#         time_of_first_top_after_boundary, first_top_after_boundary = \
#             boundary_next_top(boundary.end, boundary, single_timeframe_peaks, TREND.BULLISH)
#     elif boundary['bull_bear_side'] == TREND.BEARISH.value:
#         time_of_last_top_before_boundary, value_of_last_top_before_boundary = \
#             boundary_previous_top(start, boundary, single_timeframe_peaks, TREND.BEARISH)
#         time_of_first_top_after_boundary, first_top_after_boundary = \
#             boundary_next_top(boundary.end, boundary, single_timeframe_valleys, TREND.BEARISH)
#     else:
#         raise Exception(f"Invalid boundary['bull_bear_side']={boundary['bull_bear_side']}")
#     movement_start_value = boundary[f'internal_{movement_start_column}']
#     movement_start_time = boundary[f'{movement_start_column}_time']
#     if value_of_last_top_before_boundary is not None:
#         if boundary_adjacent_top_is_stronger(value_of_last_top_before_boundary, boundary, movement_start_column):
#             movement_start_value = value_of_last_top_before_boundary[movement_start_column]
#             movement_start_time = time_of_last_top_before_boundary
#     movement_end_value = boundary[f'internal_{movement_end_column}']
#     movement_end_time = boundary[f'{movement_end_column}_time']
#     if first_top_after_boundary is not None:
#         if boundary_adjacent_top_is_stronger(first_top_after_boundary, boundary, movement_end_column):
#             movement_end_value = first_top_after_boundary[movement_end_column]
#             movement_end_time = time_of_first_top_after_boundary
#     return movement_start_value, movement_end_value, movement_start_time, movement_end_time


# def old_bull_bear_boundary_movement(boundary: pd.Series, start: datetime, single_timeframe_peaks: pd.DataFrame,
#                                     single_timeframe_valleys: pd.DataFrame) \
#         -> Tuple[float, float, pd.Timestamp, pd.Timestamp]:
#     """
#     Calculate the movement range for a bullish or bearish boundary.
#
#     This function calculates the start and end points of the movement range for a given bullish or bearish boundary.
#     The start point is determined based on the previous valley before the boundary, and the end point is determined
#     based on the next peak after the boundary.
#
#     Parameters:
#         boundary (pd.Series): The boundary for which to calculate the movement range.
#         start (pd.Timestamp): The timestamp index of the boundary.
#         single_timeframe_peaks (pd.DataFrame): DataFrame containing peaks for the same timeframe as the boundary.
#         single_timeframe_valleys (pd.DataFrame): DataFrame containing valleys for the same timeframe as the boundary.
#
#     Returns:
#         Tuple[float, float, pd.Timestamp, pd.Timestamp]: A tuple containing the start value, end value,
#                                                         start time, and end time of the movement range.
#
#     Raises:
#         Exception: If the 'bull_bear_side' value in the boundary is not valid.
#
#     Example:
#         # Calculate the movement range for a bullish boundary
#         boundary_series = single_timeframe_peaks.loc[boundary_index]
#         start_val, end_val, start_time, end_time = bull_bear_boundary_movement(boundary_series, boundary_index,
#                                                                                single_timeframe_peaks,
#                                                                                single_timeframe_valleys)
#         print(f"Start of movement value: {start_val}, End of movement value: {end_val}")
#         print(f"Start of movement time: {start_time}, End of movement time: {end_time}")
#     """
#     if boundary['bull_bear_side'] == TREND.BULLISH.value:
#         high_low = 'high'
#         reverse_high_low = 'low'
#         time_of_last_top_before_boundary, value_of_last_top_before_boundary = \
#             boundary_previous_top(start, boundary, single_timeframe_valleys, TREND.BULLISH)
#         time_of_first_top_after_boundary, first_top_after_boundary = \
#             boundary_next_top(boundary.end, boundary, single_timeframe_peaks, TREND.BULLISH)
#     elif boundary['bull_bear_side'] == TREND.BEARISH.value:
#         high_low = 'low'
#         reverse_high_low = 'high'
#         time_of_last_top_before_boundary, value_of_last_top_before_boundary = \
#             boundary_previous_top(start, boundary, single_timeframe_peaks, TREND.BEARISH)
#         time_of_first_top_after_boundary, first_top_after_boundary = \
#             boundary_next_top(boundary.end, boundary, single_timeframe_valleys, TREND.BEARISH)
#     else:
#         raise Exception(f"Invalid boundary['bull_bear_side']={boundary['bull_bear_side']}")
#     movement_start_value = boundary[f'internal_{reverse_high_low}']
#     movement_start_time = boundary[f'{reverse_high_low}_time']
#     if value_of_last_top_before_boundary is not None:
#         if boundary_adjacent_top_is_stronger(value_of_last_top_before_boundary, boundary, reverse_high_low):
#             movement_start_value = value_of_last_top_before_boundary[reverse_high_low]
#             movement_start_time = time_of_last_top_before_boundary
#     movement_end_value = boundary[f'internal_{high_low}']
#     movement_end_time = boundary[f'{high_low}_time']
#     if first_top_after_boundary is not None:
#         if boundary_adjacent_top_is_stronger(first_top_after_boundary, boundary, high_low):
#             movement_end_value = first_top_after_boundary[high_low]
#             movement_end_time = time_of_first_top_after_boundary
#     return movement_start_value, movement_end_value, movement_start_time, movement_end_time


# def boundary_adjacent_top_is_stronger(adjacent_top, boundary, high_or_low):
#     if high_or_low == 'high':
#         return adjacent_top[high_or_low] > boundary[f'internal_{high_or_low}']
#     elif high_or_low == 'low':
#         return adjacent_top[high_or_low] < boundary[f'internal_{high_or_low}']
#     else:
#         raise Exception(f'Invalid high_low:{high_or_low}')


# def zz_next_top_of_boundary(boundary, single_timeframe_peaks_n_valleys, top_type: TopTYPE):
#     next_peak_after_boundary = single_timeframe_peaks_n_valleys[
#         (single_timeframe_peaks_n_valleys.index.get_level_values('date') > boundary.end) &
#         (single_timeframe_peaks_n_valleys.peak_or_valley == top_type.value)
#         ].head(1)
#     last_peak_inside_boundary = single_timeframe_peaks_n_valleys[
#         (single_timeframe_peaks_n_valleys.index.get_level_values('date') <= boundary.end) &
#         (single_timeframe_peaks_n_valleys.peak_or_valley == top_type.value)
#         ].tail(1)
#     return last_peak_inside_boundary, next_peak_after_boundary


# def boundary_previous_top(boundary_start: datetime, boundary: pd.Series, single_timeframe_peaks_or_valleys,
#                           trend: TREND) \
#         -> (pd.Timestamp, pd.Series):
#     # todo: we left here
#     index_of_best_top_before_boundary = None
#     # todo: eliminate .sort_index(level='date')
#     previous_tops = single_timeframe_peaks_or_valleys.loc[
#         single_timeframe_peaks_or_valleys.index.get_level_values('date') < boundary_start
#         ].sort_index(level='date')
#     if len(previous_tops) == 0:
#         return None, None
#     if trend == TREND.BULLISH:
#         if previous_tops.iloc[-1]['low'] > boundary['internal_low']:
#             return None, None
#         for top_i_index in range(len(previous_tops) - 1, 0, -1):
#             if previous_tops.iloc[top_i_index - 1]['low'] > previous_tops.iloc[top_i_index]['low']:
#                 index_of_best_top_before_boundary = previous_tops.index[top_i_index]
#                 break
#     elif trend == TREND.BEARISH:
#         if previous_tops.iloc[-1]['high'] < boundary['internal_high']:
#             return None, None
#         for top_i_index in range(len(previous_tops) - 1, 0, -1):
#             if previous_tops.iloc[top_i_index - 1]['high'] < previous_tops.iloc[top_i_index]['high']:
#                 index_of_best_top_before_boundary = previous_tops.index[top_i_index]
#                 break
#     else:
#         raise Exception(f'Invalid trend type: {trend}')
#     if index_of_best_top_before_boundary is None:
#         return None, None
#     else:
#         return index_of_best_top_before_boundary[1], single_timeframe_peaks_or_valleys.loc[
#             index_of_best_top_before_boundary]
#
#     #     non_significant_previous_valleys = single_timeframe_peaks_or_valleys[
#     #         (single_timeframe_peaks_or_valleys.index.get_level_values('date') < boundary_start)
#     #         & (single_timeframe_peaks_or_valleys['low'] > boundary['internal_low'])
#     #         ].sort_index(level='date').index.get_level_values('date')
#     #     if len(non_significant_previous_valleys) > 0:
#     #         first_non_significant_previous_valley = non_significant_previous_valleys[-1]
#     #         more_significant_tops = single_timeframe_peaks_or_valleys[
#     #             (single_timeframe_peaks_or_valleys.index.get_level_values('date') < boundary_start)
#     #             & (single_timeframe_peaks_or_valleys.index.get_level_values('date') >
#     #                first_non_significant_previous_valley)
#     #             ]
#     #         if len(more_significant_tops) > 0:
#     #             index_of_best_top_before_boundary = more_significant_tops['low'].idxmin()
#     # elif trend == TREND.BEARISH:
#     #     non_significant_previous_peak = single_timeframe_peaks_or_valleys[
#     #         (single_timeframe_peaks_or_valleys.index.get_level_values('date') < boundary_start)
#     #         & (single_timeframe_peaks_or_valleys['high'] < boundary['internal_high'])
#     #         ].sort_index(level='date').index.get_level_values('date')
#     #     if len(non_significant_previous_peak) > 0:
#     #         first_non_significant_previous_peak = non_significant_previous_peak[-1]
#     #         more_significant_tops = single_timeframe_peaks_or_valleys[
#     #             (single_timeframe_peaks_or_valleys.index.get_level_values('date') < boundary_start)
#     #             & (single_timeframe_peaks_or_valleys.index.get_level_values(
#     #                 'date') > first_non_significant_previous_peak)
#     #             ]
#     #         if len(more_significant_tops) > 0:
#     #             index_of_best_top_before_boundary = more_significant_tops['high'].idxmax()
#     # else:
#     #     raise Exception(f'Invalid trend type: {trend}')
#     # if index_of_best_top_before_boundary is None:
#     #     return None, None
#     # else:
#     #     return index_of_best_top_before_boundary[1], single_timeframe_peaks_or_valleys.loc[
#     #         index_of_best_top_before_boundary]
#
#     # previous_peak_before_boundary = single_timeframe_peaks_or_valleys[
#     #     (single_timeframe_peaks_or_valleys.index.get_level_values('date') < boundary_start)
#     # ].sort_index(level='date').tail(1)
#     # if len(previous_peak_before_boundary) == 1:
#     #     return previous_peak_before_boundary.index.get_level_values('date')[-1], \
#     #         previous_peak_before_boundary.iloc[-1]
#     # if len(previous_peak_before_boundary) == 0:
#     #     return None, None
#     # else:
#     #     raise Exception('Unhandled situation!')


# def boundary_next_top(boundary_end: pd.Timestamp, boundary, single_timeframe_peaks_or_valleys, trend: TREND) \
#         -> (pd.Timestamp, pd.Series):
#     # todo: eliminate .sort_index(level='date')
#     index_of_best_top_after_boundary = None
#     next_tops = single_timeframe_peaks_or_valleys.loc[
#         single_timeframe_peaks_or_valleys.index.get_level_values('date') > boundary_end
#         ].sort_index(level='date')
#     if len(next_tops) == 0:
#         return None, None
#     if trend == TREND.BULLISH:
#         if next_tops.iloc[0]['high'] < boundary['internal_high']:
#             return None, None
#         for top_i_index in range(len(next_tops) - 1):
#             if next_tops.iloc[top_i_index + 1]['high'] < next_tops.iloc[top_i_index]['high']:
#                 index_of_best_top_after_boundary = next_tops.index[top_i_index]
#                 break
#     elif trend == TREND.BEARISH:
#         if next_tops.iloc[0]['low'] > boundary['internal_low']:
#             return None, None
#         for top_i_index in range(len(next_tops) - 1):
#             if next_tops.iloc[top_i_index + 1]['low'] > next_tops.iloc[top_i_index]['low']:
#                 index_of_best_top_after_boundary = next_tops.index[top_i_index]
#                 break
#     else:
#         raise Exception(f'Invalid trend type: {trend}')
#     if index_of_best_top_after_boundary is None:
#         return None, None
#     else:
#         return index_of_best_top_after_boundary[1], single_timeframe_peaks_or_valleys.loc[
#             index_of_best_top_after_boundary]
#
#     # index_of_best_top_after_boundary = None
#     # if trend == TREND.BULLISH:
#     #     non_significant_in_next_peaks = single_timeframe_peaks_or_valleys[
#     #         (single_timeframe_peaks_or_valleys.index.get_level_values('date') > boundary_end)
#     #         & (single_timeframe_peaks_or_valleys['high'] < boundary['internal_high'])
#     #         ].sort_index(level='date').index.get_level_values('date')
#     #     if len(non_significant_in_next_peaks) > 0:
#     #         first_non_significant_next_peak = non_significant_in_next_peaks[0]
#     #         more_significant_tops = single_timeframe_peaks_or_valleys[
#     #             (single_timeframe_peaks_or_valleys.index.get_level_values('date') > boundary_end)
#     #             & (single_timeframe_peaks_or_valleys.index.get_level_values('date') < first_non_significant_next_peak)
#     #             ]
#     #         if len(more_significant_tops) > 0:
#     #             index_of_best_top_after_boundary = more_significant_tops['high'].idxmax()
#     # elif trend == TREND.BEARISH:
#     #     non_significant_next_valleys = single_timeframe_peaks_or_valleys[
#     #         (single_timeframe_peaks_or_valleys.index.get_level_values('date') > boundary_end)
#     #         & (single_timeframe_peaks_or_valleys['low'] > boundary['internal_low'])
#     #         ].sort_index(level='date').index.get_level_values('date')
#     #     if len(non_significant_next_valleys) > 0:
#     #         first_non_significant_next_valley = non_significant_next_valleys[0]
#     #         more_significant_tops = single_timeframe_peaks_or_valleys[
#     #             (single_timeframe_peaks_or_valleys.index.get_level_values('date') > boundary_end)
#     #             & (single_timeframe_peaks_or_valleys.index.get_level_values('date') < first_non_significant_next_valley)
#     #             ]
#     #         if len(more_significant_tops) > 0:
#     #             index_of_best_top_after_boundary = more_significant_tops['low'].idxmin()
#     # else:
#     #     raise Exception(f'Invalid trend type: {trend}')
#     # if index_of_best_top_after_boundary is None:
#     #     return None, None
#     # else:
#     #     return index_of_best_top_after_boundary[1], single_timeframe_peaks_or_valleys.loc[
#     #         index_of_best_top_after_boundary]
#
#     # next_peak_before_boundary = single_timeframe_peaks_or_valleys[
#     #     (single_timeframe_peaks_or_valleys.index.get_level_values('date') > boundary_end)
#     # ].sort_index(level='date').head(1)
#     # if len(next_peak_before_boundary) == 1:
#     #     return next_peak_before_boundary.index.get_level_values('date')[0], \
#     #         next_peak_before_boundary.iloc[0]
#     # if len(next_peak_before_boundary) == 0:
#     #     return None, None
#     # else:
#     #     raise Exception('Unhandled situation!')


@measure_time
def multi_timeframe_bull_bear_side_trends(multi_timeframe_candle_trend: pd.DataFrame,
                                          multi_timeframe_peaks_n_valleys: pd.DataFrame,
                                          multi_timeframe_ohlcva: pd.DataFrame,
                                          timeframe_shortlist: List['str'] = None) \
        -> pt.DataFrame[MultiTimeframeBullBearSide]:
    trends = empty_df(MultiTimeframeBullBearSide)

    if timeframe_shortlist is None:
        timeframe_shortlist = config.timeframes
    for timeframe in timeframe_shortlist:
        timeframe_candle_trend = single_timeframe(multi_timeframe_candle_trend, timeframe)
        if len(timeframe_candle_trend) == 0:
            log(f'multi_timeframe_candle_trend has no rows for '
                f'{timeframe}/{date_range_of_data(multi_timeframe_ohlcva)}',
                stack_trace=False, severity=LogSeverity.WARNING)
            continue
        timeframe_peaks_n_valleys = major_peaks_n_valleys(multi_timeframe_peaks_n_valleys, timeframe)
        timeframe_trends = single_timeframe_bull_bear_side_trends(timeframe_candle_trend,
                                                                  timeframe_peaks_n_valleys,
                                                                  single_timeframe(multi_timeframe_ohlcva, timeframe)
                                                                  , timeframe)
        timeframe_trends['timeframe'] = timeframe
        timeframe_trends.set_index('timeframe', append=True, inplace=True)
        timeframe_trends = timeframe_trends.swaplevel().sort_index(level='date')
        trends = concat(trends, timeframe_trends)
    trends = cast_and_validate(trends, MultiTimeframeBullBearSide)
    return trends


@measure_time
def add_trend_extremum(_boundaries, single_timeframe_peak_n_valley: pt.DataFrame[PeakValley],
                       ohlcv: pt.DataFrame[OHLCV]):
    _boundaries['internal_high'] = pd.Series(dtype=float)
    _boundaries['high_time'] = pd.Series(dtype='datetime64[ns, UTC]')
    _boundaries['internal_low'] = pd.Series(dtype=float)
    _boundaries['low_time'] = pd.Series(dtype='datetime64[ns, UTC]')
    if len(_boundaries) == 0:
        return _boundaries
    _boundaries = _boundaries.copy()
    single_timeframe_peak_n_valley.reset_index(level='timeframe', inplace=True)
    _boundaries = _boundaries[:-1]
    for start, _boundary in _boundaries.iterrows():
        # boundary_internal_tops = single_timeframe_peak_n_valley[
        #     (start < single_timeframe_peak_n_valley.index.get_level_values('date'))
        #     & (single_timeframe_peak_n_valley.index.get_level_values('date') < _boundary['end'])]
        # if len(boundary_internal_tops) > 0:
        #     _boundaries.loc[start, 'internal_high'] = boundary_internal_tops['high'].max()
        #     _boundaries.loc[start, 'high_time'] = (boundary_internal_tops['high'].idxmax())
        #     _boundaries.loc[start, 'internal_low'] = boundary_internal_tops['low'].min()
        #     _boundaries.loc[start, 'low_time'] = (boundary_internal_tops['low'].idxmin())
        # else:
        _boundaries.loc[start, 'internal_high'] = ohlcv.loc[start:_boundary['end'], 'high'].max()
        _boundaries.loc[start, 'high_time'] = (ohlcv.loc[start:_boundary['end'], 'high'].idxmax())
        _boundaries.loc[start, 'internal_low'] = ohlcv.loc[start:_boundary['end'], 'low'].min()
        _boundaries.loc[start, 'low_time'] = (ohlcv.loc[start:_boundary['end'], 'low'].idxmin())
    return _boundaries


# def add_trend_tops(_boundaries, single_timeframe_candle_trend):
#     if 'internal_high' not in _boundaries.columns:
#         _boundaries['internal_high'] = None
#     if 'internal_low' not in _boundaries.columns:
#         _boundaries['internal_low'] = None
#     if 'high_time' not in _boundaries.columns:
#         _boundaries['high_time'] = None
#     if 'low_time' not in _boundaries.columns:
#         _boundaries['low_time'] = None
#     for i, _boundary in _boundaries.iterrows():
#         _boundaries.loc[i, 'internal_high'] = single_timeframe_candle_trend.loc[i:_boundary['end'], 'high'].max()
#         _boundaries.loc[i, 'high_time'] = single_timeframe_candle_trend.loc[i:_boundary['end'], 'high'].idxmax()
#         _boundaries.loc[i, 'internal_low'] = single_timeframe_candle_trend.loc[i:_boundary['end'], 'low'].min()
#         _boundaries.loc[i, 'low_time'] = single_timeframe_candle_trend.loc[i:_boundary['end'], 'low'].idxmin()
#     return _boundaries


def most_two_significant_tops(start, end, single_timeframe_peaks_n_valleys, tops_type: TopTYPE) -> pd.DataFrame:
    # todo: test most_two_significant_valleys
    filtered_valleys = single_timeframe_peaks_n_valleys.loc[
        (single_timeframe_peaks_n_valleys.index >= start) &
        (single_timeframe_peaks_n_valleys.index <= end) &
        (single_timeframe_peaks_n_valleys['peak_or_valley'] == tops_type.value)
        ].sort_values(by='strength')
    return filtered_valleys.iloc[:2].sort_index(level='date')


def trend_ATRs(timeframe_boundaries: pt.DataFrame[BullBearSide], ohlcva: pt.DataFrame[OHLCVA]):
    _boundaries_ATRs = [boundary_ATRs(_boundary_index, _boundary['end'], ohlcva) for _boundary_index, _boundary in
                        timeframe_boundaries.iterrows()]

    for _i, _ATRs in enumerate(_boundaries_ATRs):
        if len(_ATRs) == 0:
            raise Exception(f'Boundary with no ATRs! '
                            f'({timeframe_boundaries.index[_i]}:{timeframe_boundaries.iloc[_i, "end"]})')
    # min = [min(_ATRs) for _ATRs in _boundaries_ATRs]
    # max = [max(_ATRs) for _ATRs in _boundaries_ATRs]
    # average = [sum(_ATRs) / len(_ATRs) for _ATRs in _boundaries_ATRs]

    return [sum(_ATRs[_ATRs.notnull()]) / len(_ATRs[_ATRs.notnull()]) for _ATRs in _boundaries_ATRs]


def trend_rate(_boundaries: pt.DataFrame[BullBearSide], timeframe: str) -> pt.DataFrame[BullBearSide]:
    return _boundaries['movement'] / (_boundaries['duration'] / pd.to_timedelta(timeframe))


def trend_duration(_boundaries: pt.DataFrame[BullBearSide]) -> pt.Series[timedelta]:
    durations = pd.to_datetime(_boundaries['end']) - pd.to_datetime(_boundaries.index)
    for index, duration in durations.items():
        if not duration > timedelta(0):
            raise Exception(f'Duration must be greater than zero. But @{index}={duration}. in: {_boundaries}')
    return durations


def ignore_weak_trend(_boundaries: pt.DataFrame[BullBearSide]) -> pt.DataFrame[BullBearSide]:
    """
    Remove weak trends from the DataFrame.

    Parameters:
        _boundaries (pt.DataFrame[Model.BullBearSide.BullBearSide]): A DataFrame containing trend boundary data.

    Returns:
        pt.DataFrame[Model.BullBearSide.BullBearSide]: A DataFrame with weak trends removed.

    Example:
        # Assuming you have a DataFrame '_boundaries' with the required columns and data
        filtered_boundaries = ignore_weak_trend(_boundaries)
    """
    _boundaries.loc[_boundaries['strength'] < config.momentum_trand_strength_factor, 'bull_bear_side'] \
        = TREND.SIDE.value
    return _boundaries


def trend_movement(_boundaries: pt.DataFrame[BullBearSide]) -> pt.Series[float]:
    """
        Calculate the trend movement as the difference between the highest high and the lowest low.

        Parameters:
            _boundaries (pd.DataFrame): A DataFrame containing trend boundary data.

        Returns:
            pd.Series: A Series containing the calculated trend movement values.

        Example:
            # Assuming you have a DataFrame '_boundaries' with the required columns
            result = trend_movement(_boundaries)
        """
    if len(_boundaries) > 0:
        t = _boundaries[['internal_high', 'movement_start_value', 'movement_end_value']].max(axis='columns') \
            - _boundaries[['internal_low', 'movement_start_value', 'movement_end_value']].min(axis='columns')
        return t
    else:
        return pd.Series(dtype=float)


def trend_strength(_boundaries):
    return _boundaries['rate'] / _boundaries['ATR']


def previous_trend(trends: pt.DataFrame[BullBearSide]) -> Tuple[List[Optional[int]], List[Optional[float]]]:
    """
        Find the previous trend and its movement for each row in a DataFrame of trends.

        Args:
            trends (pd.DataFrame): A DataFrame containing trend data with columns 'movement_start_time' and 'movement_end_time'.

        Returns:
            Tuple[List[Optional[int]], List[Optional[float]]]: A tuple containing two lists:
                - A list of previous trend indices (int or None).
                - A list of the corresponding previous trend movements (float or None).
        """
    _previous_trends = []
    _previous_trends_movement = []
    for _start, this_trend in trends.iterrows():
        if this_trend['movement_start_time'] is not None:
            possible_previous_trends = trends[trends['movement_end_time'] == this_trend['movement_start_time']]
            if len(possible_previous_trends) > 0:
                _previous_trends.append(possible_previous_trends['movement'].idxmax())
                _previous_trends_movement.append(possible_previous_trends['movement'].max())
                continue
            else:
                log(f'did not find any previous trend for trend stat@{bull_bear_side_repr(_start, this_trend)})',
                    stack_trace=False)
        else:
            raise Exception(f"movement_start_time is not valid:{this_trend['movement_start_time']}")
        _previous_trends.append(None)
        _previous_trends_movement.append(None)
    return _previous_trends, _previous_trends_movement


@measure_time
def single_timeframe_bull_bear_side_trends(single_timeframe_candle_trend: pd.DataFrame,
                                           single_timeframe_peaks_n_valleys, ohlcva: pt.DataFrame[OHLCVA],
                                           timeframe: str) -> pd.DataFrame:
    if ohlcva['ATR'].first_valid_index() is None:
        # return pd.DataFrame()
        return empty_df(BullBearSide)
    single_timeframe_candle_trend = single_timeframe_candle_trend.loc[ohlcva['ATR'].first_valid_index():]
    _trends = detect_trends(single_timeframe_candle_trend, timeframe)
    _trends = add_trend_extremum(_trends, single_timeframe_peaks_n_valleys, ohlcva)
    _trends = expand_trends_by_near_tops(_trends, single_timeframe_peaks_n_valleys)
    _trends['movement'] = trend_movement(_trends)
    _trends['ATR'] = trend_ATRs(_trends, ohlcva=ohlcva)
    assert not _trends['ATR'].isnull().any()
    _trends['duration'] = trend_duration(_trends)
    _trends['rate'] = trend_rate(_trends, timeframe)
    _trends['strength'] = trend_strength(_trends)
    _trends = cast_and_validate(_trends, BullBearSide, zero_size_allowed=True)
    return _trends


def boundary_ATRs(start: Timestamp, end: Timestamp, ohlcva: pt.DataFrame[OHLCVA]) -> List[float]:
    _boundary_ATRs = ohlcva.loc[start:end, 'ATR']
    if len(_boundary_ATRs[_boundary_ATRs.notnull()]) <= 0:
        raise Exception(f'Boundaries expected to be generated over candles with ATR but in '
                        f'{start}-{end} there is not any valid ATR!')
    return ohlcva.loc[start:end, 'ATR']  # .fillna(0)


def detect_trends(single_timeframe_candle_trend, timeframe: str) -> pt.DataFrame[BullBearSide]:
    single_timeframe_candle_trend = single_timeframe_candle_trend.copy()
    if len(single_timeframe_candle_trend) < 2:
        _boundaries = single_timeframe_candle_trend
        _boundaries['bull_bear_side'] = pd.NA
        _boundaries['end'] = pd.Series(dtype='datetime64[ns, UTC]')
        return _boundaries[['bull_bear_side', 'end']]
    # single_timeframe_candle_trend.loc[single_timeframe_candle_trend.index[1]:, 'time_of_previous'] = \
    #     single_timeframe_candle_trend.index[:-1]
    single_timeframe_candle_trend['previous_trend'] = single_timeframe_candle_trend['bull_bear_side'].shift(1)
    _boundaries = single_timeframe_candle_trend[
        single_timeframe_candle_trend['previous_trend'] != single_timeframe_candle_trend['bull_bear_side']
        ].copy()
    _boundaries['end'] = pd.Series(dtype='datetime64[ns, UTC]')
    time_of_last_candle = single_timeframe_candle_trend.index.get_level_values('date')[-1]
    if len(_boundaries) > 1:
        _boundaries.loc[:_boundaries.index[-2], 'end'] = \
            pd.to_datetime(to_timeframe(_boundaries.index.get_level_values('date')[1:], timeframe).tolist())
    _boundaries.loc[_boundaries.index[-1], 'end'] = to_timeframe(time_of_last_candle, timeframe)
    assert not any(_boundaries.iloc[:-1]['end'].isna())
    if _boundaries.iloc[-1]['end'] == _boundaries.index[-1]:
        _boundaries.drop(_boundaries.index[-1], inplace=True)
    return _boundaries[['bull_bear_side', 'end']]


def read_multi_timeframe_bull_bear_side_trends(date_range_str: str = None) -> pt.DataFrame[MultiTimeframeBullBearSide]:
    result = read_file(
        date_range_str,
        'multi_timeframe_bull_bear_side_trends',
        generate_multi_timeframe_bull_bear_side_trends,
        MultiTimeframeBullBearSide)
    return result


def read_multi_timeframe_candle_trend(date_range_str: str = None):
    result = read_file(date_range_str, 'multi_timeframe_candle_trend', generate_multi_timeframe_candle_trend,
                       MultiTimeframeCandleTrend)
    return result


@measure_time
def generate_multi_timeframe_bull_bear_side_trends(date_range_str: str = None, file_path: str = config.path_of_data,
                                                   timeframe_shortlist: List['str'] = None):
    if date_range_str is None:
        date_range_str = config.processing_date_range
    multi_timeframe_ohlcva = read_multi_timeframe_ohlcva(date_range_str)

    multi_timeframe_peaks_n_valleys = read_multi_timeframe_peaks_n_valleys(date_range_str)
    multi_timeframe_candle_trend = read_multi_timeframe_candle_trend(date_range_str)
    trends = multi_timeframe_bull_bear_side_trends(multi_timeframe_candle_trend,
                                                   multi_timeframe_peaks_n_valleys,
                                                   multi_timeframe_ohlcva,
                                                   timeframe_shortlist=timeframe_shortlist)
    # # Plot multi-timeframe trend boundaries
    # plot_multi_timeframe_bull_bear_side_trends(multi_timeframe_ohlcva, multi_timeframe_peaks_n_valleys, trends,
    #                                            timeframe_shortlist=timeframe_shortlist)
    # Save multi-timeframe trend boundaries to a.zip file
    trends.sort_index(inplace=True, level='date')
    trends.to_csv(os.path.join(file_path, f'multi_timeframe_bull_bear_side_trends.{date_range_str}.zip'),
                  compression='zip')


@measure_time
def generate_multi_timeframe_candle_trend(date_range_str: str, timeframe_shortlist: List['str'] = None,
                                          file_path: str = config.path_of_data):
    multi_timeframe_ohlcv = read_multi_timeframe_ohlcv(date_range_str)
    multi_timeframe_peaks_n_valleys = read_multi_timeframe_peaks_n_valleys(date_range_str).sort_index(level='date')
    multi_timeframe_candle_trend = empty_df(MultiTimeframeCandleTrend)
    if timeframe_shortlist is None:
        timeframe_shortlist = config.timeframes
    for timeframe in timeframe_shortlist:  # peaks_n_valleys.index.unique(level='timeframe'):
        _timeframe_candle_trend = \
            single_timeframe_candles_trend(single_timeframe(multi_timeframe_ohlcv, timeframe),
                                           major_peaks_n_valleys(multi_timeframe_peaks_n_valleys, timeframe))
        if len(_timeframe_candle_trend) > 0:
            _timeframe_candle_trend['timeframe'] = timeframe
            _timeframe_candle_trend.set_index('timeframe', append=True, inplace=True)
            _timeframe_candle_trend = _timeframe_candle_trend.swaplevel()
            multi_timeframe_candle_trend = concat(multi_timeframe_candle_trend, _timeframe_candle_trend)
    multi_timeframe_candle_trend.sort_index(inplace=True, level='date')
    multi_timeframe_candle_trend.to_csv(os.path.join(file_path, f'multi_timeframe_candle_trend.{date_range_str}.zip'),
                                        compression='zip')
    return multi_timeframe_candle_trend

# def merge_retracing_trends():
#     """
#     if:
#         2 BULL/BEAR trends of the same direction separated only by at most one SIDE trend
#         SIDE trend movement is less than 1 ATR
#         SIDE trend duration is less than 3 full candles
#     then:
#         merge 2 BULL/BEAR trends together
#         remove SIDE trend
#     if 2 BULL/BEAR trends of the same direction
#     :return:
#     """
#     # todo: implement merge_retracing_trends
#     raise Exception('Not implemented')
#
#
