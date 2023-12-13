import os
from datetime import datetime, timedelta
from typing import Tuple, List, Optional

import pandas as pd
import pandera.typing as pt
from pandas import Timestamp

import PeakValley
from Config import TopTYPE, config, TREND
from Model.BullBearSide import BullBearSide
from Model.MultiTimeframeBullBearSide import MultiTimeframeBullBearSide
from Model.MultiTimeframeCandleTrend import MultiTimeframeCandleTrend
from Model.MultiTimeframeOHLCV import OHLCV
from Model.MultiTimeframeOHLCVA import OHLCVA
from PeakValley import peaks_only, valleys_only, read_multi_timeframe_peaks_n_valleys, major_peaks_n_valleys, \
    insert_previous_n_next_top
from atr import read_multi_timeframe_ohlcva
from data_preparation import read_file, single_timeframe, to_timeframe, cast_and_validate, empty_df
from helper import log, measure_time
from ohlcv import read_multi_timeframe_ohlcv


@measure_time
def insert_previous_n_next_tops(single_timeframe_peaks_n_valleys: pt.DataFrame[PeakValley], ohlcv: pt.DataFrame[OHLCV]) \
        -> pt.DataFrame[OHLCV]:
    ohlcv = insert_previous_n_next_top(TopTYPE.PEAK, single_timeframe_peaks_n_valleys, ohlcv)
    ohlcv = insert_previous_n_next_top(TopTYPE.VALLEY, single_timeframe_peaks_n_valleys, ohlcv)
    return ohlcv


def single_timeframe_candles_trend(ohlcv: pt.DataFrame[OHLCV],
                                   single_timeframe_peaks_n_valley: pt.DataFrame[PeakValley]) -> pd.DataFrame:
    # Todo: Not tested!
    candle_trend = insert_previous_n_next_tops(single_timeframe_peaks_n_valley, ohlcv)
    candle_trend['bull_bear_side'] = TREND.SIDE.value
    candle_trend.loc[
        candle_trend.index[
            (candle_trend['next_valley_value'] > candle_trend['previous_valley_value'])
            & (candle_trend['next_peak_value'] > candle_trend['previous_peak_value'])
            # & (candle_trend['next_peak_index'] > candle_trend['next_valley_index'])
            ],  # the higher peak should be after higher valley
        'bull_bear_side'] = TREND.BULLISH.value
    candle_trend.loc[
        candle_trend.index[
            (candle_trend['next_peak_value'] < candle_trend['previous_peak_value'])
            & (candle_trend['next_valley_value'] < candle_trend['previous_valley_value'])
            # & (candle_trend['next_peak_index'] > candle_trend['next_valley_index'])
            ],  # the lower valley should be after lower peak
        'bull_bear_side'] = TREND.BEARISH.value
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


def add_toward_top_to_trend(_single_timeframe_bull_bear_side_trends: pt.DataFrame[BullBearSide],
                            single_timeframe_peaks_n_valleys: pd.DataFrame,
                            timeframe: str) -> pd.DataFrame:
    """
    Expand boundaries towards the previous trend top.

    This function expands boundaries that are not of 'SIDE' trend towards the previous top of the same trend
    (either bullish or bearish) inside the boundary. If the trend is bullish, it finds the next peak (first peak
    after the boundary finishes) and the last peak inside the boundary. If the next peak's high value is greater
    than the last peak, the boundary is expanded to include the next peak. Similarly, for a bearish trend, it finds
    the next valley (first valley after the boundary finishes) and the last valley inside the boundary. If the
    next valley's low value is lower than the last valley, the boundary is expanded to include the next valley.

    Parameters:
        _single_timeframe_bull_bear_side_trends (pd.DataFrame): DataFrame containing boundaries for the specified timeframe.
        single_timeframe_peaks_n_valleys (pd.DataFrame): DataFrame containing peaks and valleys for the same timeframe.
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
    # todo: does not work correctly
    if not _single_timeframe_bull_bear_side_trends.index.is_unique:
        raise Exception('We expect the single_timeframe_boundaries index be unique but isn\'t. '
                        'So we may have unexpected results after renaming DataFrame index to move boundary start.')
    single_timeframe_peaks = peaks_only(single_timeframe_peaks_n_valleys).sort_index(level='date')
    single_timeframe_valleys = valleys_only(single_timeframe_peaks_n_valleys).sort_index(level='date')
    boundary_index: Timestamp
    for boundary_index, boundary in _single_timeframe_bull_bear_side_trends[
        _single_timeframe_bull_bear_side_trends['bull_bear_side'].isin([TREND.BULLISH.value, TREND.BEARISH.value])
    ].iterrows():
        movement_start_value, movement_end_value, movement_start_time, movement_end_time = \
            bull_bear_boundary_movement(boundary, boundary_index,
                                        single_timeframe_peaks,
                                        single_timeframe_valleys)
        _single_timeframe_bull_bear_side_trends.loc[boundary_index, 'movement_start_value'] = movement_start_value
        _single_timeframe_bull_bear_side_trends.loc[boundary_index, 'movement_end_value'] = movement_end_value
        _single_timeframe_bull_bear_side_trends.loc[boundary_index, 'movement_start_time'] = movement_start_time
        _single_timeframe_bull_bear_side_trends.loc[boundary_index, 'movement_end_time'] = movement_end_time
    return _single_timeframe_bull_bear_side_trends


def bull_bear_boundary_movement(boundary: pd.Series, start: datetime, single_timeframe_peaks: pd.DataFrame,
                                single_timeframe_valleys: pd.DataFrame) \
        -> Tuple[float, float, pd.Timestamp, pd.Timestamp]:
    """
    Calculate the movement range for a bullish or bearish boundary.

    This function calculates the start and end points of the movement range for a given bullish or bearish boundary.
    The start point is determined based on the previous valley before the boundary, and the end point is determined
    based on the next peak after the boundary.

    Parameters:
        boundary (pd.Series): The boundary for which to calculate the movement range.
        start (pd.Timestamp): The timestamp index of the boundary.
        single_timeframe_peaks (pd.DataFrame): DataFrame containing peaks for the same timeframe as the boundary.
        single_timeframe_valleys (pd.DataFrame): DataFrame containing valleys for the same timeframe as the boundary.

    Returns:
        Tuple[float, float, pd.Timestamp, pd.Timestamp]: A tuple containing the start value, end value,
                                                        start time, and end time of the movement range.

    Raises:
        Exception: If the 'bull_bear_side' value in the boundary is not valid.

    Example:
        # Calculate the movement range for a bullish boundary
        boundary_series = single_timeframe_peaks.loc[boundary_index]
        start_val, end_val, start_time, end_time = bull_bear_boundary_movement(boundary_series, boundary_index,
                                                                               single_timeframe_peaks,
                                                                               single_timeframe_valleys)
        print(f"Start of movement value: {start_val}, End of movement value: {end_val}")
        print(f"Start of movement time: {start_time}, End of movement time: {end_time}")
    """
    if boundary['bull_bear_side'] == TREND.BULLISH.value:
        high_low = 'high'
        reverse_high_low = 'low'
        time_of_last_top_before_boundary, last_top_before_boundary = \
            previous_top_of_boundary(start, boundary, single_timeframe_valleys, TREND.BULLISH)
        time_of_first_top_after_boundary, first_top_after_boundary = \
            next_top_of_boundary(boundary.end, boundary, single_timeframe_peaks, TREND.BULLISH)
    elif boundary['bull_bear_side'] == TREND.BEARISH.value:
        high_low = 'low'
        reverse_high_low = 'high'
        time_of_last_top_before_boundary, last_top_before_boundary = \
            previous_top_of_boundary(start, boundary, single_timeframe_peaks, TREND.BEARISH)
        time_of_first_top_after_boundary, first_top_after_boundary = \
            next_top_of_boundary(boundary.end, boundary, single_timeframe_valleys, TREND.BEARISH)
    else:
        raise Exception(f"Invalid boundary['bull_bear_side']={boundary['bull_bear_side']}")
    movement_start_value = boundary[f'internal_{reverse_high_low}']
    movement_start_time = boundary[f'{reverse_high_low}_time']
    if last_top_before_boundary is not None:
        if boundary_adjacent_top_is_stronger(last_top_before_boundary, boundary, reverse_high_low):
            movement_start_value = last_top_before_boundary[reverse_high_low]
            movement_start_time = time_of_last_top_before_boundary
    movement_end_value = boundary[f'internal_{high_low}']
    movement_end_time = boundary[f'{high_low}_time']
    if first_top_after_boundary is not None:
        if boundary_adjacent_top_is_stronger(first_top_after_boundary, boundary, high_low):
            movement_end_value = first_top_after_boundary[high_low]
            movement_end_time = time_of_first_top_after_boundary
    return movement_start_value, movement_end_value, movement_start_time, movement_end_time


def boundary_adjacent_top_is_stronger(adjacent_top, boundary, high_or_low):
    if high_or_low == 'high':
        return adjacent_top[high_or_low] > boundary[f'internal_{high_or_low}']
    elif high_or_low == 'low':
        return adjacent_top[high_or_low] < boundary[f'internal_{high_or_low}']
    else:
        raise Exception(f'Invalid high_low:{high_or_low}')


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


def previous_top_of_boundary(boundary_start: datetime, boundary: pd.Series, single_timeframe_peaks_or_valleys,
                             trend: TREND) \
        -> (pd.Timestamp, pd.Series):
    index_of_best_top_before_boundary = None
    # todo: eliminate .sort_index(level='date')
    previous_tops = single_timeframe_peaks_or_valleys.loc[
        single_timeframe_peaks_or_valleys.index.get_level_values('date') < boundary_start
        ].sort_index(level='date')
    if len(previous_tops) == 0:
        return None, None
    if trend == TREND.BULLISH:
        if previous_tops.iloc[-1]['low'] > boundary['internal_low']:
            return None, None
        for top_i_index in range(len(previous_tops) - 1, 0, -1):
            if previous_tops.iloc[top_i_index - 1]['low'] > previous_tops.iloc[top_i_index]['low']:
                index_of_best_top_before_boundary = previous_tops.index[top_i_index]
                break
    elif trend == TREND.BEARISH:
        if previous_tops.iloc[-1]['high'] < boundary['internal_high']:
            return None, None
        for top_i_index in range(len(previous_tops) - 1, 0, -1):
            if previous_tops.iloc[top_i_index - 1]['high'] < previous_tops.iloc[top_i_index]['high']:
                index_of_best_top_before_boundary = previous_tops.index[top_i_index]
                break
    else:
        raise Exception(f'Invalid trend type: {trend}')
    if index_of_best_top_before_boundary is None:
        return None, None
    else:
        return index_of_best_top_before_boundary[1], single_timeframe_peaks_or_valleys.loc[
            index_of_best_top_before_boundary]

    #     non_significant_previous_valleys = single_timeframe_peaks_or_valleys[
    #         (single_timeframe_peaks_or_valleys.index.get_level_values('date') < boundary_start)
    #         & (single_timeframe_peaks_or_valleys['low'] > boundary['internal_low'])
    #         ].sort_index(level='date').index.get_level_values('date')
    #     if len(non_significant_previous_valleys) > 0:
    #         first_non_significant_previous_valley = non_significant_previous_valleys[-1]
    #         more_significant_tops = single_timeframe_peaks_or_valleys[
    #             (single_timeframe_peaks_or_valleys.index.get_level_values('date') < boundary_start)
    #             & (single_timeframe_peaks_or_valleys.index.get_level_values('date') >
    #                first_non_significant_previous_valley)
    #             ]
    #         if len(more_significant_tops) > 0:
    #             index_of_best_top_before_boundary = more_significant_tops['low'].idxmin()
    # elif trend == TREND.BEARISH:
    #     non_significant_previous_peak = single_timeframe_peaks_or_valleys[
    #         (single_timeframe_peaks_or_valleys.index.get_level_values('date') < boundary_start)
    #         & (single_timeframe_peaks_or_valleys['high'] < boundary['internal_high'])
    #         ].sort_index(level='date').index.get_level_values('date')
    #     if len(non_significant_previous_peak) > 0:
    #         first_non_significant_previous_peak = non_significant_previous_peak[-1]
    #         more_significant_tops = single_timeframe_peaks_or_valleys[
    #             (single_timeframe_peaks_or_valleys.index.get_level_values('date') < boundary_start)
    #             & (single_timeframe_peaks_or_valleys.index.get_level_values(
    #                 'date') > first_non_significant_previous_peak)
    #             ]
    #         if len(more_significant_tops) > 0:
    #             index_of_best_top_before_boundary = more_significant_tops['high'].idxmax()
    # else:
    #     raise Exception(f'Invalid trend type: {trend}')
    # if index_of_best_top_before_boundary is None:
    #     return None, None
    # else:
    #     return index_of_best_top_before_boundary[1], single_timeframe_peaks_or_valleys.loc[
    #         index_of_best_top_before_boundary]

    # previous_peak_before_boundary = single_timeframe_peaks_or_valleys[
    #     (single_timeframe_peaks_or_valleys.index.get_level_values('date') < boundary_start)
    # ].sort_index(level='date').tail(1)
    # if len(previous_peak_before_boundary) == 1:
    #     return previous_peak_before_boundary.index.get_level_values('date')[-1], \
    #         previous_peak_before_boundary.iloc[-1]
    # if len(previous_peak_before_boundary) == 0:
    #     return None, None
    # else:
    #     raise Exception('Unhandled situation!')


def next_top_of_boundary(boundary_end: pd.Timestamp, boundary, single_timeframe_peaks_or_valleys, trend: TREND) \
        -> (pd.Timestamp, pd.Series):
    # todo: eliminate .sort_index(level='date')
    index_of_best_top_after_boundary = None
    next_tops = single_timeframe_peaks_or_valleys.loc[
        single_timeframe_peaks_or_valleys.index.get_level_values('date') > boundary_end
        ].sort_index(level='date')
    if len(next_tops) == 0:
        return None, None
    if trend == TREND.BULLISH:
        if next_tops.iloc[0]['high'] < boundary['internal_high']:
            return None, None
        for top_i_index in range(len(next_tops) - 1):
            if next_tops.iloc[top_i_index + 1]['high'] < next_tops.iloc[top_i_index]['high']:
                index_of_best_top_after_boundary = next_tops.index[top_i_index]
                break
    elif trend == TREND.BEARISH:
        if next_tops.iloc[0]['low'] > boundary['internal_low']:
            return None, None
        for top_i_index in range(len(next_tops) - 1):
            if next_tops.iloc[top_i_index + 1]['low'] > next_tops.iloc[top_i_index]['low']:
                index_of_best_top_after_boundary = next_tops.index[top_i_index]
                break
    else:
        raise Exception(f'Invalid trend type: {trend}')
    if index_of_best_top_after_boundary is None:
        return None, None
    else:
        return index_of_best_top_after_boundary[1], single_timeframe_peaks_or_valleys.loc[
            index_of_best_top_after_boundary]

    # index_of_best_top_after_boundary = None
    # if trend == TREND.BULLISH:
    #     non_significant_in_next_peaks = single_timeframe_peaks_or_valleys[
    #         (single_timeframe_peaks_or_valleys.index.get_level_values('date') > boundary_end)
    #         & (single_timeframe_peaks_or_valleys['high'] < boundary['internal_high'])
    #         ].sort_index(level='date').index.get_level_values('date')
    #     if len(non_significant_in_next_peaks) > 0:
    #         first_non_significant_next_peak = non_significant_in_next_peaks[0]
    #         more_significant_tops = single_timeframe_peaks_or_valleys[
    #             (single_timeframe_peaks_or_valleys.index.get_level_values('date') > boundary_end)
    #             & (single_timeframe_peaks_or_valleys.index.get_level_values('date') < first_non_significant_next_peak)
    #             ]
    #         if len(more_significant_tops) > 0:
    #             index_of_best_top_after_boundary = more_significant_tops['high'].idxmax()
    # elif trend == TREND.BEARISH:
    #     non_significant_next_valleys = single_timeframe_peaks_or_valleys[
    #         (single_timeframe_peaks_or_valleys.index.get_level_values('date') > boundary_end)
    #         & (single_timeframe_peaks_or_valleys['low'] > boundary['internal_low'])
    #         ].sort_index(level='date').index.get_level_values('date')
    #     if len(non_significant_next_valleys) > 0:
    #         first_non_significant_next_valley = non_significant_next_valleys[0]
    #         more_significant_tops = single_timeframe_peaks_or_valleys[
    #             (single_timeframe_peaks_or_valleys.index.get_level_values('date') > boundary_end)
    #             & (single_timeframe_peaks_or_valleys.index.get_level_values('date') < first_non_significant_next_valley)
    #             ]
    #         if len(more_significant_tops) > 0:
    #             index_of_best_top_after_boundary = more_significant_tops['low'].idxmin()
    # else:
    #     raise Exception(f'Invalid trend type: {trend}')
    # if index_of_best_top_after_boundary is None:
    #     return None, None
    # else:
    #     return index_of_best_top_after_boundary[1], single_timeframe_peaks_or_valleys.loc[
    #         index_of_best_top_after_boundary]

    # next_peak_before_boundary = single_timeframe_peaks_or_valleys[
    #     (single_timeframe_peaks_or_valleys.index.get_level_values('date') > boundary_end)
    # ].sort_index(level='date').head(1)
    # if len(next_peak_before_boundary) == 1:
    #     return next_peak_before_boundary.index.get_level_values('date')[0], \
    #         next_peak_before_boundary.iloc[0]
    # if len(next_peak_before_boundary) == 0:
    #     return None, None
    # else:
    #     raise Exception('Unhandled situation!')


@measure_time
def multi_timeframe_bull_bear_side_trends(multi_timeframe_candle_trend: pd.DataFrame,
                                          multi_timeframe_peaks_n_valleys: pd.DataFrame,
                                          multi_timeframe_ohlcva: pd.DataFrame,
                                          timeframe_shortlist: List['str'] = None) \
        -> pt.DataFrame[MultiTimeframeBullBearSide]:
    # trends = pd.DataFrame()
    trends = empty_df(MultiTimeframeBullBearSide)

    if timeframe_shortlist is None:
        timeframe_shortlist = config.timeframes
    for timeframe in timeframe_shortlist:
        timeframe_candle_trend = single_timeframe(multi_timeframe_candle_trend, timeframe)
        if len(timeframe_candle_trend) == 0:
            log(f'multi_timeframe_candle_trend has no rows for timeframe:{timeframe}')
            continue
        timeframe_peaks_n_valleys = major_peaks_n_valleys(multi_timeframe_peaks_n_valleys, timeframe)
        timeframe_trends = single_timeframe_bull_bear_side_trends(timeframe_candle_trend,
                                                                  timeframe_peaks_n_valleys,
                                                                  single_timeframe(multi_timeframe_ohlcva, timeframe)
                                                                  , timeframe)
        timeframe_trends['timeframe'] = timeframe
        timeframe_trends.set_index('timeframe', append=True, inplace=True)
        timeframe_trends = timeframe_trends.swaplevel().sort_index(level='date')
        if len(timeframe_trends) > 0:
            trends = pd.concat([trends, timeframe_trends])
    # trends = trends[
    #     [column for column in config.multi_timeframe_bull_bear_side_trends_columns if column != 'timeframe']]
    # trends = trends.astype({
    #     'end': np.datetime64,
    #     'bull_bear_side': "string",
    #     'ATR': np.float64,
    #     'internal_high': np.float64, 'internal_low': np.float64, 'high_time': np.datetime64, 'low_time': np.datetime64,
    #     'movement_start_value': 'float', 'movement_end_value': 'float',
    #     'movement_start_time': np.datetime64, 'movement_end_time': np.datetime64,
    #     'movement': 'float'
    # })
    # MultiTimeframeBullBearSide.validate(trends)
    trends = cast_and_validate(trends, MultiTimeframeBullBearSide)
    return trends


def add_trend_tops(_boundaries, single_timeframe_peak_n_valley: pt.DataFrame[PeakValley], ohlcv: pt.DataFrame[OHLCV]):
    if 'internal_high' not in _boundaries.columns:
        _boundaries['internal_high'] = None
    if 'internal_low' not in _boundaries.columns:
        _boundaries['internal_low'] = None
    if 'high_time' not in _boundaries.columns:
        _boundaries['high_time'] = None
    if 'low_time' not in _boundaries.columns:
        _boundaries['low_time'] = None
    for i, _boundary in _boundaries.iterrows():
        boundary_internal_tops = single_timeframe_peak_n_valley[
            (i < single_timeframe_peak_n_valley.index.get_level_values('date'))
            & (single_timeframe_peak_n_valley.index.get_level_values('date') < _boundary['end'])]
        if len(boundary_internal_tops) > 0:
            _boundaries.loc[i, 'internal_high'] = boundary_internal_tops['high'].max()
            _boundaries.loc[i, 'high_time'] = boundary_internal_tops['high'].idxmax()[1]
            _boundaries.loc[i, 'internal_low'] = boundary_internal_tops['low'].min()
            _boundaries.loc[i, 'low_time'] = boundary_internal_tops['low'].idxmin()[1]
        else:
            if not isinstance(_boundary['end'], datetime) or pd.isna(_boundary['end']):
                raise Exception("Check before to see why 'end' is not a valid date!")
            _boundaries.loc[i, 'internal_high'] = ohlcv.loc[i:_boundary['end'], 'high'].max()
            _boundaries.loc[i, 'high_time'] = ohlcv.loc[i:_boundary['end'], 'high'].idxmax()
            _boundaries.loc[i, 'internal_low'] = ohlcv.loc[i:_boundary['end'], 'low'].min()
            _boundaries.loc[i, 'low_time'] = ohlcv.loc[i:_boundary['end'], 'low'].idxmin()
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


def add_canal_lines(_boundaries, single_timeframe_peaks_n_valleys):
    # todo: test add_canal_lines
    if _boundaries is None:
        return _boundaries
    for i, _boundary in _boundaries.iterrows():
        if _boundary['bull_bear_side'] == TREND.SIDE.value:
            continue
        _peaks = most_two_significant_tops(i, _boundary['end'], single_timeframe_peaks_n_valleys, TopTYPE.PEAK)
        _valleys = most_two_significant_tops(i, _boundary['end'], single_timeframe_peaks_n_valleys, TopTYPE.VALLEY)
        if _boundary['bull_bear_side'] == TREND.BULLISH.value:
            canal_top = _peaks.iloc[0]['high']
            trend_tops = _valleys['low'].to_numpy()
        else:
            canal_top = _valleys.iloc[0]['low']
            trend_tops = _peaks['high'].to_numpy()
        trend_acceleration = (trend_tops[1][1] - trend_tops[0][1]) / (trend_tops[1][0] - trend_tops[0][0])
        trend_base = trend_tops[0][1] - trend_tops[0][0] * trend_acceleration
        canal_acceleration = trend_acceleration
        canal_base = canal_top[0][1] - canal_top[0][0] * canal_acceleration
        _boundaries.iloc[i, ['trend_acceleration', 'trend_base', 'canal_acceleration', 'canal_base']] = \
            [trend_acceleration, trend_base, canal_acceleration, canal_base]
    return _boundaries


def trend_ATRs(single_timeframe_boundaries: pt.DataFrame[BullBearSide], ohlcva: pt.DataFrame[OHLCVA]):
    _boundaries_ATRs = [boundary_ATRs(_boundary_index, _boundary['end'], ohlcva) for _boundary_index, _boundary in
                        single_timeframe_boundaries.iterrows()]

    for _i, _ATRs in enumerate(_boundaries_ATRs):
        if len(_ATRs) == 0:
            raise Exception(f'Boundary with no ATRs! '
                            f'({single_timeframe_boundaries.index[_i]}:{single_timeframe_boundaries.iloc[_i, "end"]})')
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
        _boundaries (pt.DataFrame[BullBearSide]): A DataFrame containing trend boundary data.

    Returns:
        pt.DataFrame[BullBearSide]: A DataFrame with weak trends removed.

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
    return _boundaries[['internal_high', 'movement_start_value', 'movement_end_value']].max(axis='columns') \
        - _boundaries[['internal_low', 'movement_start_value', 'movement_end_value']].min(axis='columns')


def trend_strength(_boundaries):
    return _boundaries['rate'] / _boundaries['ATR']


def test_boundary_ATR(_boundaries: pt.DataFrame[BullBearSide]) -> bool:
    if _boundaries['ATR'].isnull().any():
        raise Exception(f'Nan ATR in: {_boundaries[_boundaries["ATR"].isna()]}')
    return True


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
                log(f'did not find any previous trend for trend stat@{_start}({this_trend})')
        else:
            raise Exception(f"movement_start_time is not valid:{this_trend['movement_start_time']}")
        _previous_trends.append(None)
        _previous_trends_movement.append(None)
    return _previous_trends, _previous_trends_movement


def single_timeframe_bull_bear_side_trends(single_timeframe_candle_trend: pd.DataFrame,
                                           single_timeframe_peaks_n_valleys, ohlcva: pt.DataFrame[OHLCVA],
                                           timeframe: str) -> pd.DataFrame:
    if ohlcva['ATR'].first_valid_index() is None:
        # return pd.DataFrame()
        return empty_df(BullBearSide)
    single_timeframe_candle_trend = single_timeframe_candle_trend.loc[ohlcva['ATR'].first_valid_index():]
    _trends = detect_trends(single_timeframe_candle_trend, timeframe)
    _trends = add_trend_tops(_trends, single_timeframe_peaks_n_valleys, ohlcva)
    _trends = add_toward_top_to_trend(_trends, single_timeframe_peaks_n_valleys, timeframe)
    _trends['movement'] = trend_movement(_trends)
    _trends['ATR'] = trend_ATRs(_trends, ohlcva=ohlcva)
    test_boundary_ATR(_trends)
    _trends['duration'] = trend_duration(_trends)
    _trends['rate'] = trend_rate(_trends, timeframe)
    _trends['strength'] = trend_strength(_trends)
    _trends = cast_and_validate(_trends, BullBearSide)
    return _trends


def boundary_ATRs(_boundary_start: Timestamp, _boudary_end: Timestamp, ohlcva: pt.DataFrame[OHLCVA]) -> List[float]:
    _boundary_ATRs = ohlcva.loc[_boundary_start:_boudary_end, 'ATR']
    if len(_boundary_ATRs[_boundary_ATRs.notnull()]) <= 0:
        raise Exception(f'Boundaries expected to be generated over candles with ATR but in '
                        f'{_boundary_start}-{_boudary_end} there is not any valid ATR!')
    return ohlcva.loc[_boundary_start:_boudary_end, 'ATR']  # .fillna(0)


def detect_trends(single_timeframe_candle_trend, timeframe: str) -> pt.DataFrame[BullBearSide]:
    single_timeframe_candle_trend = single_timeframe_candle_trend.copy()

    single_timeframe_candle_trend.loc[single_timeframe_candle_trend.index[1]:, 'time_of_previous'] = \
        single_timeframe_candle_trend.index[:-1]
    single_timeframe_candle_trend['previous_trend'] = single_timeframe_candle_trend['bull_bear_side'].shift(1)
    _boundaries = single_timeframe_candle_trend[
        single_timeframe_candle_trend['previous_trend'] != single_timeframe_candle_trend['bull_bear_side']]
    time_of_last_candle = single_timeframe_candle_trend.index.get_level_values('date')[-1]
    _boundaries = _boundaries.copy()
    if len(_boundaries) > 1:
        _boundaries.loc[:_boundaries.index[-2], 'end'] = \
            to_timeframe(_boundaries.index.get_level_values('date')[1:], timeframe)
    _boundaries.loc[_boundaries.index[-1], 'end'] = to_timeframe(time_of_last_candle, timeframe)
    _boundaries['min_ATR'] = to_timeframe(time_of_last_candle, timeframe)
    if _boundaries.iloc[-1]['end'] == _boundaries.index[-1]:
        _boundaries.drop(_boundaries.index[-1], inplace=True)

    # if 'time_of_previous' not in single_timeframe_candle_trend.columns:
    #     log('"time_of_previous" column not in the result. maybe no boundaries found!', LogSeverity.WARNING)
    #     single_timeframe_candle_trend['time_of_previous'] = None
    # if 'end' not in single_timeframe_candle_trend.columns:
    #     log('"end" column not in the result. maybe no boundaries found!', LogSeverity.WARNING)
    #     single_timeframe_candle_trend['end'] = pd.to_datetime(None)
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
    # Generate multi-timeframe candle trend

    multi_timeframe_candle_trend = read_multi_timeframe_candle_trend(date_range_str)
    # Generate multi-timeframe trend boundaries
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
        _timeframe_candle_trend['timeframe'] = timeframe
        _timeframe_candle_trend.set_index('timeframe', append=True, inplace=True)
        _timeframe_candle_trend = _timeframe_candle_trend.swaplevel()
        if len(_timeframe_candle_trend) > 0:
            if len(multi_timeframe_candle_trend) == 0:
                multi_timeframe_candle_trend = _timeframe_candle_trend
            else:
                multi_timeframe_candle_trend = pd.concat([multi_timeframe_candle_trend, _timeframe_candle_trend])
    multi_timeframe_candle_trend.sort_index(inplace=True, level='date')
    multi_timeframe_candle_trend.to_csv(os.path.join(file_path, f'multi_timeframe_candle_trend.{date_range_str}.zip'),
                                        compression='zip')
    return multi_timeframe_candle_trend


def bull_bear_side_repr(start: datetime, trend: pd.Series):
    text = f'{trend["bull_bear_side"].replace("_TREND", "")}: ' \
           f'{start.strftime("%H:%M")}-{trend["end"].strftime("%H:%M")}'
    if 'movement' in trend.keys():
        text += f'\nM:{trend["movement"]:.2f}'
    if 'duration' in trend.keys():
        text += f'D:{trend["duration"] / timedelta(hours=1):.2f}h'
    if 'strength' in trend.keys():
        text += f'S:{trend["strength"]:.2f}'
    if 'ATR' in trend.keys():
        text += f'ATR:{trend["ATR"]:.2f}'
    return text

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
