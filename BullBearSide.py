import os
from datetime import datetime, timedelta
from typing import Tuple

import pandas as pd
import pandera
import pandera.typing as pt
from pandas import Timestamp

from Candle import read_multi_timeframe_ohlc, read_multi_timeframe_ohlca
from Config import TopTYPE, config, TREND
from DataPreparation import read_file, single_timeframe, to_timeframe
from FigurePlotter.BullBearSide_plotter import plot_multi_timeframe_bull_bear_side_trends
from PeakValley import peaks_only, valleys_only, read_multi_timeframe_peaks_n_valleys, major_peaks_n_valleys, \
    PeaksValleys
from helper import log


class BullBearSide(pandera.DataFrameModel):
    date: pt.Index[datetime]  # start
    bull_bear_side: pt.Series[str]
    end: pt.Series[datetime]
    internal_high: pt.Series[float]
    high_time: pt.Series[Timestamp]
    internal_low: pt.Series[float]
    low_time: pt.Series[Timestamp]
    movement: pt.Series[float]
    strength: pt.Series[float]
    ATR: pt.Series[float]
    duration: pt.Series[timedelta]


def insert_previous_n_next_tops(single_timeframe_peaks_n_valleys, ohlc):
    ohlc = insert_previous_n_next_top(TopTYPE.PEAK, single_timeframe_peaks_n_valleys, ohlc)
    ohlc = insert_previous_n_next_top(TopTYPE.VALLEY, single_timeframe_peaks_n_valleys, ohlc)
    return ohlc


def insert_previous_n_next_top(top_type: TopTYPE, peaks_n_valleys, ohlc):
    # Todo: Not tested!
    if f'previous_{top_type.value}_index' not in ohlc.columns:
        ohlc[f'previous_{top_type.value}_index'] = None
    if f'previous_{top_type.value}_value' not in ohlc.columns:
        ohlc[f'previous_{top_type.value}_value'] = None
    if f'next_{top_type.value}_index' not in ohlc.columns:
        ohlc[f'next_{top_type.value}_index'] = None
    if f'next_{top_type.value}_value' not in ohlc.columns:
        ohlc[f'next_{top_type.value}_value'] = None
    tops = peaks_n_valleys[peaks_n_valleys['peak_or_valley'] == top_type.value]
    high_or_low = 'high' if top_type == TopTYPE.PEAK else 'low'
    for i in range(len(tops)):
        if i == len(tops) - 1:
            is_previous_for_indexes = ohlc.loc[ohlc.index > tops.index.get_level_values('date')[i]].index
        else:
            is_previous_for_indexes = ohlc.loc[(ohlc.index > tops.index.get_level_values('date')[i]) &
                                               (ohlc.index <= tops.index.get_level_values('date')[i + 1])].index
        if i == 0:
            is_next_for_indexes = ohlc.loc[ohlc.index <= tops.index.get_level_values('date')[i]].index
        else:
            is_next_for_indexes = ohlc.loc[(ohlc.index > tops.index.get_level_values('date')[i - 1]) &
                                           (ohlc.index <= tops.index.get_level_values('date')[i])].index
        ohlc.loc[is_previous_for_indexes, f'previous_{top_type.value}_index'] = tops.index.get_level_values('date')[i]
        ohlc.loc[is_previous_for_indexes, f'previous_{top_type.value}_value'] = tops.iloc[i][high_or_low]
        ohlc.loc[is_next_for_indexes, f'next_{top_type.value}_index'] = tops.index.get_level_values('date')[i]
        ohlc.loc[is_next_for_indexes, f'next_{top_type.value}_value'] = tops.iloc[i][high_or_low]
    return ohlc


def single_timeframe_candles_trend(ohlc: pd.DataFrame, single_timeframe_peaks_n_valley: pd.DataFrame) -> pd.DataFrame:
    # Todo: Not tested!
    candle_trend = insert_previous_n_next_tops(single_timeframe_peaks_n_valley, ohlc)
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


def add_toward_top_to_trend(single_timeframe_boundaries: pt.DataFrame[B],
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
        single_timeframe_boundaries (pd.DataFrame): DataFrame containing boundaries for the specified timeframe.
        single_timeframe_peaks_n_valleys (pd.DataFrame): DataFrame containing peaks and valleys for the same timeframe.
        timeframe (str): The timeframe for which the boundaries and peaks/valleys are calculated.

    Returns:
        pd.DataFrame: The input DataFrame with added columns 'start_value_of_movement' and 'end_value_of_movement'.

    Raises:
        Exception: If the DataFrame index of single_timeframe_boundaries is not unique.

    Example:
        # Expand boundaries towards the previous trend top for a specific timeframe
        updated_boundaries = add_toward_top_to_trend(single_timeframe_boundaries,
                                                     single_timeframe_peaks_n_valleys,
                                                     '15min')
        print(updated_boundaries)
    """
    if not single_timeframe_boundaries.index.is_unique:
        raise Exception('We expect the single_timeframe_boundaries index be unique but isn\'t. '
                        'So we may have unexpected results after renaming DataFrame index to move boundary start.')
    single_timeframe_peaks = peaks_only(single_timeframe_peaks_n_valleys)
    single_timeframe_valleys = valleys_only(single_timeframe_peaks_n_valleys)
    boundary_index: Timestamp
    for boundary_index, boundary in single_timeframe_boundaries[
        single_timeframe_boundaries['bull_bear_side'].isin(TREND.BULLISH.value, TREND.BEARISH.value)
    ].iterrows():
        start_value_of_movement, end_value_of_movement, start_time_of_movement, end_time_of_movement = \
            bull_bear_boundary_movement(boundary, boundary_index,
                                        single_timeframe_peaks,
                                        single_timeframe_valleys)
        single_timeframe_boundaries.loc[boundary_index, 'start_value_of_movement'] = start_value_of_movement
        single_timeframe_boundaries.loc[boundary_index, 'end_value_of_movement'] = end_value_of_movement
    return single_timeframe_boundaries


def bull_bear_boundary_movement(boundary: pd.Series, boundary_start: pd.Timestamp,
                                single_timeframe_peaks: pd.DataFrame, single_timeframe_valleys: pd.DataFrame) \
        -> Tuple[float, float, pd.Timestamp, pd.Timestamp]:
    """
    Calculate the movement range for a bullish or bearish boundary.

    This function calculates the start and end points of the movement range for a given bullish or bearish boundary.
    The start point is determined based on the previous valley before the boundary, and the end point is determined
    based on the next peak after the boundary.

    Parameters:
        boundary (pd.Series): The boundary for which to calculate the movement range.
        boundary_start (pd.Timestamp): The timestamp index of the boundary.
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
            previous_top_of_boundary(boundary_start, single_timeframe_valleys)
        time_of_first_top_after_boundary, first_top_after_boundary = \
            next_top_of_boundary(boundary.end, single_timeframe_peaks)
    elif boundary['bull_bear_side'] == TREND.BEARISH.value:
        high_low = 'low'
        reverse_high_low = 'high'
        time_of_last_top_before_boundary, last_top_before_boundary = \
            previous_top_of_boundary(boundary_start, single_timeframe_peaks)
        time_of_first_top_after_boundary, first_top_after_boundary = \
            next_top_of_boundary(boundary.end, single_timeframe_valleys)
    else:
        raise Exception(f"Invalid boundary['bull_bear_side']={boundary['bull_bear_side']}")
    start_value_of_movement = boundary[high_low]
    start_time_of_movement = boundary_start
    if last_top_before_boundary is not None:
        if boundary_adjacent_top_is_stringer(last_top_before_boundary, boundary, high_low):
            start_value_of_movement = last_top_before_boundary[high_low]
            start_time_of_movement = time_of_last_top_before_boundary
    end_value_of_movement = boundary[reverse_high_low]
    end_time_of_movement = boundary['end']
    if first_top_after_boundary is not None:
        if boundary_adjacent_top_is_stringer(first_top_after_boundary, boundary, high_low):
            end_value_of_movement = first_top_after_boundary[reverse_high_low]
            end_time_of_movement = time_of_first_top_after_boundary
    return start_value_of_movement, end_value_of_movement, start_time_of_movement, end_time_of_movement


def boundary_adjacent_top_is_stringer(adjacent_top, boundary, high_low):
    if high_low == 'high':
        return adjacent_top[high_low] < boundary[f'{high_low}est_{high_low}']
    elif high_low == 'low':
        return adjacent_top[high_low] > boundary[f'{high_low}est_{high_low}']
    else:
        raise Exception(f'Invalid high_low:{high_low}')


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


def previous_top_of_boundary(boundary_start: pd.Timestamp, single_timeframe_peaks_or_valleys) \
        -> (pd.Timestamp, pd.Series):
    previous_peak_before_boundary = single_timeframe_peaks_or_valleys[
        (single_timeframe_peaks_or_valleys.index.get_level_values('date') < boundary_start)
    ].sort_index(level='date').tail(1)
    if len(previous_peak_before_boundary) == 1:
        return previous_peak_before_boundary.index.get_level_values('date')[-1], \
            previous_peak_before_boundary.iloc[-1]
    if len(previous_peak_before_boundary) == 0:
        return None, None
    else:
        raise Exception('Unhandled situation!')


def next_top_of_boundary(boundary_end: pd.Timestamp, single_timeframe_peaks_or_valleys) \
        -> (pd.Timestamp, pd.Series):
    next_peak_before_boundary = single_timeframe_peaks_or_valleys[
        (single_timeframe_peaks_or_valleys.index.get_level_values('date') > boundary_end)
    ].sort_index(level='date').head(1)
    if len(next_peak_before_boundary) == 1:
        return next_peak_before_boundary.index.get_level_values('date')[0], \
            next_peak_before_boundary.iloc[0]
    if len(next_peak_before_boundary) == 0:
        return None, None
    else:
        raise Exception('Unhandled situation!')


@measure_time
def multi_timeframe_bull_bear_side_trends(multi_timeframe_candle_trend: pd.DataFrame,
                                          multi_timeframe_peaks_n_valleys: pd.DataFrame,
                                          multi_timeframe_ohlca: pd.DataFrame,
                                          timeframe_shortlist: List['str'] = None):
    boundaries = pd.DataFrame()
    if timeframe_shortlist is None:
        timeframe_shortlist = config.timeframes
    for timeframe in timeframe_shortlist:
        single_timeframe_candle_trend = single_timeframe(multi_timeframe_candle_trend, timeframe)
        if len(single_timeframe_candle_trend) == 0:
            log(f'multi_timeframe_candle_trend has no rows for timeframe:{timeframe}')
            continue
        single_timeframe_peaks_n_valleys = major_peaks_n_valleys(multi_timeframe_peaks_n_valleys, timeframe)
        _timeframe_trends = single_timeframe_bull_bear_side_trends(single_timeframe_candle_trend,
                                                                   single_timeframe_peaks_n_valleys,
                                                                   single_timeframe(multi_timeframe_ohlca,
                                                                                    timeframe)
                                                                   , timeframe)
        _timeframe_trends['timeframe'] = timeframe
        _timeframe_trends.set_index('timeframe', append=True, inplace=True)
        _timeframe_trends = _timeframe_trends.swaplevel()
        if len(_timeframe_trends) > 0:
            boundaries = pd.concat([boundaries, _timeframe_trends])
    boundaries = boundaries[
        [column for column in config.multi_timeframe_bull_bear_side_trends_columns if column != 'timeframe']]

    return boundaries


def add_trend_tops(_boundaries, single_timeframe_candle_trend):
    if 'internal_high' not in _boundaries.columns:
        _boundaries['internal_high'] = None
    if 'internal_low' not in _boundaries.columns:
        _boundaries['internal_low'] = None
    if 'high_time' not in _boundaries.columns:
        _boundaries['high_time'] = None
    if 'low_time' not in _boundaries.columns:
        _boundaries['low_time'] = None
    for i, _boundary in _boundaries.iterrows():
        _boundaries.loc[i, 'internal_high'] = single_timeframe_candle_trend.loc[i:_boundary['end'], 'high'].max()
        _boundaries.loc[i, 'high_time'] = single_timeframe_candle_trend.loc[i:_boundary['end'], 'high'].idxmax()
        _boundaries.loc[i, 'internal_low'] = single_timeframe_candle_trend.loc[i:_boundary['end'], 'low'].min()
        _boundaries.loc[i, 'low_time'] = single_timeframe_candle_trend.loc[i:_boundary['end'], 'low'].idxmin()
    return _boundaries


def most_two_significant_tops(start, end, single_timeframe_peaks_n_valleys, tops_type: TopTYPE) -> pd.DataFrame:
    # todo: test most_two_significant_valleys
    filtered_valleys = single_timeframe_peaks_n_valleys.loc[
        (single_timeframe_peaks_n_valleys.index >= start) &
        (single_timeframe_peaks_n_valleys.index <= end) &
        (single_timeframe_peaks_n_valleys['peak_or_valley'] == tops_type.value)
        ].sort_values(by='strength')
    return filtered_valleys.iloc[:2].sort_index()


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


def trend_ATRs(single_timeframe_boundaries: pt.DataFrame[BullBearSide], ohlca: pt.DataFrame[OHLCA]):
    _boundaries_ATRs = [boundary_ATRs(_boundary_index, _boundary['end'], ohlca) for _boundary_index, _boundary in
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
    return _boundaries['internal_high'] - _boundaries['internal_low']


def trend_strength(_boundaries):
    return _boundaries['rate'] / _boundaries['ATR']


def test_boundary_ATR(_boundaries: pt.DataFrame[BullBearSide]) -> bool:
    if _boundaries['ATR'].isnull().any():
        raise Exception(f'Nan ATR in: {_boundaries[_boundaries["ATR"].isna()]}')
    return True


def single_timeframe_bull_bear_side_trends(single_timeframe_candle_trend: pd.DataFrame,
                                           single_timeframe_peaks_n_valleys, ohlca: pt.DataFrame[OHLCA],
                                           timeframe: str) -> pd.DataFrame:
    if ohlca['ATR'].first_valid_index() is None:
        return pd.DataFrame()
    single_timeframe_candle_trend = single_timeframe_candle_trend.loc[ohlca['ATR'].first_valid_index():]
    _boundaries = detect_boundaries(single_timeframe_candle_trend, timeframe)
    _boundaries = add_trend_tops(_boundaries, single_timeframe_candle_trend)

    _boundaries = add_toward_top_to_trend(_boundaries, single_timeframe_peaks_n_valleys, timeframe)

    _boundaries['movement'] = trend_movement(_boundaries)
    _boundaries['ATR'] = trend_ATRs(_boundaries, ohlca=ohlca)
    test_boundary_ATR(_boundaries)
    _boundaries['duration'] = trend_duration(_boundaries)
    _boundaries['rate'] = trend_rate(_boundaries, timeframe)
    _boundaries['strength'] = trend_strength(_boundaries)
    # back_boundaries = _boundaries.copy()
    # _boundaries = ignore_weak_trend(_boundaries)
    # todo: test merge_overlapped_trends
    # _boundaries = merge_overlapped_single_timeframe_trends(_boundaries, timeframe)
    single_timeframe_ohlca = single_timeframe(read_multi_timeframe_ohlca(config.under_process_date_range), timeframe)
    # plot_multiple_figures([
    #     plot_single_timeframe_bull_bear_side_trends(single_timeframe_ohlca, single_timeframe_peaks_n_valleys,
    #                                                 back_boundaries, name=f'back_boundaries', save=False, show=False),
    #     plot_single_timeframe_bull_bear_side_trends(single_timeframe_ohlca, single_timeframe_peaks_n_valleys,
    #                                                 _boundaries,
    #                                                 name=f'modified_boundaries', save=False, show=False),
    # ], name='compare after adding previous toward top')
    # _boundaries = add_canal_lines(_boundaries, single_timeframe_peaks_n_valleys)
    _boundaries = _boundaries[[i for i in config.multi_timeframe_bull_bear_side_trends_columns if i != 'timeframe']]
    return _boundaries


def boundary_ATRs(_boundary_start: Timestamp, _boudary_end: Timestamp, ohlca: pt.DataFrame[OHLCA]) -> List[float]:
    _boundary_ATRs = ohlca.loc[_boundary_start:_boudary_end, 'ATR']
    if len(_boundary_ATRs[_boundary_ATRs.notnull()]) <= 0:
        raise Exception(f'Boundaries expected to be generated over candles with ATR but in '
                        f'{_boundary_start}-{_boudary_end} there is not any valid ATR!')
    return ohlca.loc[_boundary_start:_boudary_end, 'ATR']  # .fillna(0)


def detect_boundaries(single_timeframe_candle_trend, timeframe: str) -> pt.DataFrame[BullBearSide]:
    single_timeframe_candle_trend = single_timeframe_candle_trend.copy()
    if 'time_of_previous' not in single_timeframe_candle_trend.columns:
        single_timeframe_candle_trend['time_of_previous'] = None
    if 'end' not in single_timeframe_candle_trend.columns:
        single_timeframe_candle_trend['end'] = None
    # single_timeframe_candle_trend.loc[1:, 'time_of_previous'] = single_timeframe_candle_trend.index[:-1]
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
    return _boundaries[['bull_bear_side', 'end']]


def read_multi_timeframe_bull_bear_side_trends(date_range_str: str):
    return read_file(date_range_str, 'multi_timeframe_bull_bear_side_trends',
                     generate_multi_timeframe_bull_bear_side_trends)


@measure_time
def generate_multi_timeframe_bull_bear_side_trends(date_range_str: str, file_path: str = config.path_of_data,
                                                   timeframe_short_list: List['str'] = None):
    multi_timeframe_ohlca = read_multi_timeframe_ohlca(date_range_str)
    multi_timeframe_peaks_n_valleys = read_multi_timeframe_peaks_n_valleys(date_range_str)
    # Generate multi-timeframe candle trend
    multi_timeframe_candle_trend = generate_multi_timeframe_candle_trend(date_range_str,
                                                                         timeframe_shortlist=timeframe_short_list)
    # Generate multi-timeframe trend boundaries
    trends = multi_timeframe_bull_bear_side_trends(multi_timeframe_candle_trend,
                                                   multi_timeframe_peaks_n_valleys,
                                                   multi_timeframe_ohlca,
                                                   timeframe_shortlist=timeframe_short_list)
    # Plot multi-timeframe trend boundaries
    plot_multi_timeframe_bull_bear_side_trends(multi_timeframe_ohlca, multi_timeframe_peaks_n_valleys, trends,
                                               timeframe_shortlist=timeframe_short_list)
    # Save multi-timeframe trend boundaries to a.zip file
    trends.to_csv(os.path.join(file_path, f'multi_timeframe_bull_bear_side_trends.{date_range_str}.zip'),
                  compression='zip')


@measure_time
def generate_multi_timeframe_candle_trend(date_range_str: str, timeframe_shortlist: List['str'] = None):
    multi_timeframe_ohlc = read_multi_timeframe_ohlc(date_range_str)
    multi_timeframe_peaks_n_valleys = read_multi_timeframe_peaks_n_valleys(date_range_str).sort_index(level='date')
    multi_timeframe_candle_trend = pd.DataFrame()
    if timeframe_shortlist is None:
        timeframe_shortlist = config.timeframes
    for timeframe in timeframe_shortlist:  # peaks_n_valleys.index.unique(level='timeframe'):
        _timeframe_candle_trend = \
            single_timeframe_candles_trend(single_timeframe(multi_timeframe_ohlc, timeframe),
                                           major_peaks_n_valleys(multi_timeframe_peaks_n_valleys, timeframe))
        _timeframe_candle_trend['timeframe'] = timeframe
        _timeframe_candle_trend.set_index('timeframe', append=True, inplace=True)
        _timeframe_candle_trend = _timeframe_candle_trend.swaplevel()
        multi_timeframe_candle_trend = pd.concat([multi_timeframe_candle_trend, _timeframe_candle_trend])
    # multi_timeframe_candle_trend = multi_timeframe_candle_trend.sort_index()
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


def merge_retracing_trends():
    """
    if:
        2 BULL/BEAR trends of the same direction separated only by at most one SIDE trend
        SIDE trend movement is less than 1 ATR
        SIDE trend duration is less than 3 full candles
    then:
        merge 2 BULL/BEAR trends together
        remove SIDE trend
    if 2 BULL/BEAR trends of the same direction
    :return:
    """
    # todo: implement merge_retracing_trends
    raise Exception('Not implemented')


def generate_multi_timeframe_bull_bear_side_trend_pivots():
    """
    highest high of every Bullish and lowest low of every Bearish trend. for Trends
            conditions:
                >= 3 ATR
                >= 1 ATR reverse movement after the most significant top before a trend with the same direction.
                if a trend with the same direction before < 1 ATR return found, merge these together.
            index = time of pivot candle (highest high for Bullish and lowest low for Bearish) mapped to timeframe
            exact_time = exact time of pivot candle
            timeframe = time frame of pivot candle
            value = highest high for Bullish and lowest low for Bearish
            inner_margin = [Bullish: high - ]/[Bearish: low +]
                max(distance from nearest body of pivot and adjacent candles, 1 ATR in pivot timeframe)
            outer_margin =
    warning: if highest high is not the last peak of Bullish and lowest low is not the last Valley raise a warning log:
                timeframe, trend start time (index), time of last top
                time and high of highest high in Bullish and time and low of lowest low in Bearish,
    :return:
    """
    merge_retracing_trends()
    """
        in all boundaries with movement >= 3 ATR:
            if 
                movement of next boundary >= 1 ATR
                or distance of most significant peak to reverse high/low of next boundary >= 1 ATR 
                or in boundary reverse tops after most significant top find distance of >= 1 ATR
            then:
                the boundary most significant top is a static level pivot.
                if:
                    the pivot is inside a previous active or inactive level of the same or higher timeframe:
                then:
                    do not addd as a new level and increase hit count of the previous level.
                else:
                    add a new level for the pivot   
    """
    # todo: complete generate_multi_timeframe_bull_bear_side_trend_pivots
    raise Exception('Not implemented')
