import os
from datetime import datetime, timedelta
from typing import List

import pandas as pd
import pandera
import pandera.typing as pt
from pandas import Timestamp, to_timedelta

from Candle import OHLCA, read_multi_timeframe_ohlc, read_multi_timeframe_ohlca
from Config import TopTYPE, config, TREND
from DataPreparation import read_file, single_timeframe, to_timeframe
from FigurePlotter.BullBearSide_plotter import plot_single_timeframe_bull_bear_side_trends, \
    plot_multi_timeframe_bull_bear_side_trends
from FigurePlotter.plotter import plot_multiple_figures
from PeakValley import peaks_only, valleys_only, read_multi_timeframe_peaks_n_valleys, major_peaks_n_valleys, \
    PeaksValleys
from helper import log


class BullBearSide(pandera.DataFrameModel):
    date: pt.Index[datetime]  # start
    bull_bear_side: pt.Series[str]
    end: pt.Series[datetime]
    highest_high: pt.Series[float]
    high_time: pt.Series[Timestamp]
    lowest_low: pt.Series[float]
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


def add_previous_toward_trend_top_to_boundary(single_timeframe_boundaries: pt.DataFrame[BullBearSide],
                                              single_timeframe_peaks_n_valleys: pt.DataFrame[PeaksValleys],
                                              timeframe: str) -> pt.DataFrame:
    """
    if trend is not side
        if trend is bullish
            find next_peak (first peak after the boundary finishes) and last_peak inside of boundary
            if  next_peak['high'] > last_peak
                expand boundary to include next_peak
        else: # if trend is bullish
            find next_valley (first valley after the boundary finishes) and last_valley inside of boundary
            if  next_peak['high'] > last_peak
                expand boundary to include next_valley
    """
    if not single_timeframe_boundaries.index.is_unique:
        raise Exception('We expect the single_timeframe_boundaries index be unique but isn\'t. '
                        'So we may have unexpected results after renaming DataFrame index to move boundary start.')
    single_timeframe_peaks = peaks_only(single_timeframe_peaks_n_valleys)
    single_timeframe_valleys = valleys_only(single_timeframe_peaks_n_valleys)
    boundary_index: Timestamp
    for boundary_index, boundary in single_timeframe_boundaries.iterrows():
        # if boundary_index == Timestamp('2017-10-06 02:15:00'):
        #     pass
        if boundary.bull_bear_side == TREND.BULLISH.value:
            last_valley_before_boundary_time, last_valley_before_boundary = \
                previous_top_of_boundary(boundary_index, single_timeframe_valleys)
            if last_valley_before_boundary_time is not None:
                if last_valley_before_boundary['low'] < boundary['lowest_low']:
                    last_valley_before_boundary_time_mapped = to_timeframe(last_valley_before_boundary_time, timeframe)
                    single_timeframe_boundaries \
                        .rename(index={boundary_index: last_valley_before_boundary_time_mapped},
                                inplace=True)
                    single_timeframe_boundaries.loc[last_valley_before_boundary_time_mapped, 'lowest_low'] = \
                        last_valley_before_boundary['low']
        elif boundary.bull_bear_side == TREND.BEARISH.value:
            last_peak_before_boundary_time, last_peak_before_boundary = \
                previous_top_of_boundary(boundary_index, single_timeframe_peaks)
            if last_peak_before_boundary_time is not None:
                if last_peak_before_boundary['high'] > boundary['highest_high']:
                    last_peak_before_boundary_time_mapped = to_timeframe(last_peak_before_boundary_time, timeframe)
                    single_timeframe_boundaries \
                        .rename(index={boundary_index: last_peak_before_boundary_time_mapped},
                                inplace=True)
                    single_timeframe_boundaries.loc[last_peak_before_boundary_time_mapped, 'highest_high'] = \
                        last_peak_before_boundary['high']
    return single_timeframe_boundaries


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
        boundaries = pd.concat([boundaries, _timeframe_trends])
    boundaries = boundaries[
        [column for column in config.multi_timeframe_bull_bear_side_trends_columns if column != 'timeframe']]

    return boundaries


def add_trend_tops(_boundaries, single_timeframe_candle_trend):
    if 'highest_high' not in _boundaries.columns:
        _boundaries['highest_high'] = None
    if 'lowest_low' not in _boundaries.columns:
        _boundaries['lowest_low'] = None
    if 'high_time' not in _boundaries.columns:
        _boundaries['high_time'] = None
    if 'low_time' not in _boundaries.columns:
        _boundaries['low_time'] = None
    for i, _boundary in _boundaries.iterrows():
        _boundaries.loc[i, 'highest_high'] = single_timeframe_candle_trend.loc[i:_boundary['end'], 'high'].max()
        _boundaries.loc[i, 'high_time'] = single_timeframe_candle_trend.loc[i:_boundary['end'], 'high'].idxmax()
        _boundaries.loc[i, 'lowest_low'] = single_timeframe_candle_trend.loc[i:_boundary['end'], 'low'].min()
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
    durations = _boundaries['end'] - _boundaries.index
    for index, duration in durations.items():
        if not duration > timedelta(0):
            raise Exception(f'Duration must be greater than zero. But @{index}={duration}. in: {_boundaries}')
    return _boundaries['end'] - _boundaries.index


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
    return _boundaries['highest_high'] - _boundaries['lowest_low']


def trend_strength(_boundaries):
    return _boundaries['rate'] / _boundaries['ATR']


def test_boundary_ATR(_boundaries: pt.DataFrame[BullBearSide]) -> bool:
    if _boundaries['ATR'].isnull().any():
        raise Exception(f'Nan ATR in: {_boundaries[_boundaries["ATR"].isna()]}')
    return True


def single_timeframe_bull_bear_side_trends(single_timeframe_candle_trend: pd.DataFrame,
                                           single_timeframe_peaks_n_valleys, ohlca: pt.DataFrame[OHLCA],
                                           timeframe: str) -> pd.DataFrame:
    single_timeframe_candle_trend = single_timeframe_candle_trend.loc[ohlca['ATR'].first_valid_index():]
    _boundaries = detect_boundaries(single_timeframe_candle_trend, timeframe)
    _boundaries = add_trend_tops(_boundaries, single_timeframe_candle_trend)

    # _boundaries = add_previous_toward_trend_top_to_boundary(_boundaries, single_timeframe_peaks_n_valleys, timeframe)

    _boundaries['movement'] = trend_movement(_boundaries)
    _boundaries['ATR'] = trend_ATRs(_boundaries, ohlca=ohlca)
    test_boundary_ATR(_boundaries)
    _boundaries['duration'] = trend_duration(_boundaries)
    _boundaries['rate'] = trend_rate(_boundaries, timeframe)
    _boundaries['strength'] = trend_strength(_boundaries)
    back_boundaries = _boundaries.copy()
    # _boundaries = ignore_weak_trend(_boundaries)
    # todo: test merge_overlapped_trends
    # _boundaries = merge_overlapped_single_timeframe_trends(_boundaries, timeframe)
    single_timeframe_ohlca = single_timeframe(read_multi_timeframe_ohlca(config.under_process_date_range), timeframe)
    plot_multiple_figures([
        plot_single_timeframe_bull_bear_side_trends(single_timeframe_ohlca, single_timeframe_peaks_n_valleys,
                                                    back_boundaries, name=f'back_boundaries', save=False, show=False),
        plot_single_timeframe_bull_bear_side_trends(single_timeframe_ohlca, single_timeframe_peaks_n_valleys,
                                                    _boundaries,
                                                    name=f'modified_boundaries', save=False, show=False),
    ], name='compare after adding previous toward top')
    # _boundaries = add_canal_lines(_boundaries, single_timeframe_peaks_n_valleys)
    _boundaries = _boundaries[[i for i in config.multi_timeframe_bull_bear_side_trends_columns if i != 'timeframe']]
    return _boundaries


def boundary_ATRs(_boundary_start: Timestamp, _boudary_end: Timestamp, ohlca: pt.DataFrame[OHLCA]) -> List[float]:
    return ohlca.loc[_boundary_start:_boudary_end, 'ATR']  # .fillna(0)


def detect_boundaries(single_timeframe_candle_trend, timeframe: str) -> pt.DataFrame[BullBearSide]:
    if 'time_of_previous' not in single_timeframe_candle_trend.columns:
        single_timeframe_candle_trend['time_of_previous'] = None
    single_timeframe_candle_trend.loc[1:, 'time_of_previous'] = single_timeframe_candle_trend.index[:-1]
    single_timeframe_candle_trend['previous_trend'] = single_timeframe_candle_trend['bull_bear_side'].shift(1)
    _boundaries = single_timeframe_candle_trend[
        single_timeframe_candle_trend['previous_trend'] != single_timeframe_candle_trend['bull_bear_side']]
    time_of_last_candle = single_timeframe_candle_trend.index.get_level_values('date')[-1]
    if 'end' not in single_timeframe_candle_trend.columns:
        single_timeframe_candle_trend['end'] = None
    _boundaries.loc[:-1, 'end'] = to_timeframe(_boundaries.index.get_level_values('date')[1:], timeframe)
    _boundaries.loc[_boundaries.index[-1], 'end'] = to_timeframe(time_of_last_candle, timeframe)
    _boundaries['min_ATR'] = to_timeframe(time_of_last_candle, timeframe)
    if _boundaries.iloc[-1]['end'] == _boundaries.index[-1]:
        _boundaries.drop(_boundaries.index[-1], inplace=True)
    return _boundaries[['bull_bear_side', 'end']]


def read_multi_timeframe_bull_bear_side_trends(date_range_str: str):
    return read_file(date_range_str, 'multi_timeframe_bull_bear_side_trends',
                     generate_multi_timeframe_bull_bear_side_trends)


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


def merge_overlapped_single_timeframe_trends(trends: pt.DataFrame[BullBearSide], timeframe: str):
    """
    Merge overlapped trends.

    If 2 trends of the same kind (BULLISH, BEARISH, SIDE already defined in TRENDS enum and compare values) overlapped
    (have at least one candle in common) merge them.
    If a SIDE trend overlaps with a BULLISH or BEARISH trend, we move the start (index) and end of SIDE trend to remove
    overlap.
    If a SIDE trend is completely covered by a BULLISH or BEARISH trend, remove the SIDE trend.
    Just the start (index) of BULLISH/BEARISH trends can overlap with end of another BULLISH/BEARISH of the reverse
    type. Else: raise exception.

    :param timeframe:
    :param trends: DataFrame containing trend data.
                      Columns: ['end', 'direction']
                      Index: 'start' timestamp of the trend
    :return: Merged and cleaned trends DataFrame.
    """
    # todo: test merge_overlapped_trends
    # merged_trends = pd.DataFrame()
    trends.sort_index(inplace=True)
    for _idx in range(len(trends) - 1):
        start = trends.index[_idx]
        trend = trends.iloc[_idx]
        end = trend['end']
        direction = trend['bull_bear_side']
        rest_of_trends = trends.loc[(trends.index != start) | ((trends.index == start) & (trends['end'] != end))]
        # Find overlapping trends of the same kind
        same_kind_overlaps = rest_of_trends[
            (rest_of_trends.index <= end) & (rest_of_trends['end'] >= start) & (
                        rest_of_trends['bull_bear_side'] == direction)
            ]

        trends_to_drop = []

        # Merge overlapping trends of the same kind into one trend
        if len(same_kind_overlaps) > 0:
            """
            date: pt.Index[datetime]  # start
            bull_bear_side: pt.Series[str]
            end: pt.Series[datetime]
            highest_high: pt.Series[float]
            high_time: pt.Series[Timestamp]
            lowest_low: pt.Series[float]
            low_time: pt.Series[Timestamp]
            movement: pt.Series[float]
            strength: pt.Series[float]
            ATR: pt.Series[float]
            duration: pt.Series[timedelta]
            """
            same_kind_overlaps.loc[start] = trend
            trends_to_merge = same_kind_overlaps.sort_index()
            start_of_merged = trends_to_merge.index[0]
            end_of_merged = trends_to_merge['end'].max()
            bull_bear_side_of_merged = direction
            highest_high = trends_to_merge['highest_high'].max()
            time_of_highest = trends_to_merge['highest_high'].idxmax()
            high_time = trends_to_merge.loc[time_of_highest, 'high_time']
            lowest_low = trends_to_merge['lowest_low'].min()
            low_time = trends_to_merge.loc[trends_to_merge['low_time'].idxmin(), 'low_time']
            movement = highest_high - lowest_low
            duration = end_of_merged - start_of_merged
            strength = movement / (duration / to_timedelta(timeframe))
            atr = sum(trends_to_merge['ATR'] / trends_to_merge['duration'])
            merged_trend = pd.Series({
                'end': end_of_merged,
                'bull_bear_side': bull_bear_side_of_merged,
                'end': end_of_merged,
                'highest_high': highest_high,
                'high_time': high_time,
                'lowest_low': lowest_low,
                'low_time': low_time,
                'movement': movement,
                'strength': strength,
                'ATR': atr,
                'duration': duration,
            })
            trends.loc[start_of_merged] = merged_trend
            trends_to_drop.append(trends_to_merge.index)
    trends.drop(list(set(trends_to_drop)))
    for _idx in range(len(trends) - 1):
        start = trends.index[_idx]
        trend = trends.iloc[_idx]
        direction = trend['bull_bear_side']
        rest_of_trends = trends.loc[(trends.index != start) | ((trends.index == start) & (trends['end'] != end))]
        if direction == TREND.SIDE.value:

            # Check if a SIDE trend overlaps with a BULLISH or BEARISH trend
            new_start, new_end = start, trend['end']
            start_overlapping_bull_bears = rest_of_trends[
                (rest_of_trends.index <= start)
                & (rest_of_trends['end'] > start)
                # & (trends_df['bull_bear_side'].isin([TREND.BULLISH.value, TREND.BEARISH.value]))
                ]
            if len(start_overlapping_bull_bears) > 0:
                # make sure none of SIDE trends overlaps with other SIDE trends
                test_side_trend_not_overlapping_another_side(start, trend, start_overlapping_bull_bears)
                new_start = start_overlapping_bull_bears['end'].max() + to_timedelta(timeframe)
                trends.rename({start: new_start}, inplace=True)

            end_overlapping_bull_bear = rest_of_trends[
                (rest_of_trends.index <= trend['end'])
                & (rest_of_trends['end'] > trend['end'])
                # & (trends_df['bull_bear_side'].isin([TREND.BULLISH.value, TREND.BEARISH.value]))
                ]

            if len(end_overlapping_bull_bear) > 0:
                # make sure none of SIDE trends overlaps with other SIDE trends
                test_side_trend_not_overlapping_another_side(start, trend, end_overlapping_bull_bear)
                new_end = end_overlapping_bull_bear.index.min() - to_timedelta(timeframe)
                # trends.loc[(trends.index == start)&(trends['end']==trend['end']), 'end'] = new_end
                trends.iloc[_idx]['end'] = new_end

            # drop trend if end before start
            if new_end <= new_start:
                log(f'{bull_bear_side_repr(start, trend)} removed as covered by: '
                    f'{[bull_bear_side_repr(o_start, o_trend) for o_start, o_trend in start_overlapping_bull_bears.iterrows()]}')
                trends.drop(new_start)
                continue

            # drop SIDE trends completely covered by a BULLISH or BEARISH trend
            covering_trends = rest_of_trends[(trends.index <= start) & (trends['end'] >= new_end)]
            if len(covering_trends) > 0:
                trends.drop(new_start)
                continue

    return trends


def test_bull_bear_side_trends(trends: pt.DataFrame[BullBearSide]):
    for start, trend in trends.iterrows():
        end = trend['end']
        direction = trend['bull_bear_side']

        trend_overlaps = trends[
            (trends.index <= end) & (trends['end'] >= start)
            ]
        if len(trend_overlaps) > 0:
            # none of trends should overlap with another trend of same direction
            if direction in trend_overlaps['bull_bear_side']:
                _msg = f"{bull_bear_side_repr(start, trend)} overlaps with same direction trends:"
                _msg = str.join([bull_bear_side_repr(o_start, o_trend) for o_start, o_trend in
                                 trend_overlaps[trend_overlaps['bull_bear_side'] == direction].iterrows()])
                raise Exception(_msg)
            if TREND.SIDE.value in trend_overlaps['bull_bear_side']:
                _msg = f"{bull_bear_side_repr(start, trend)} overlaps with SIDE trends:"
                _msg = str.join([bull_bear_side_repr(o_start, o_trend) for o_start, o_trend in
                                 trend_overlaps[trend_overlaps['bull_bear_side'] == direction].iterrows()])
                raise Exception(_msg)
            # at this point we are sure the trend only overlaps with a reverse BULL/BEAR trend
            start_overlaps = trends[
                (trends.index <= end) & (trends.index >= start)
                ]
            end_overlaps = trends[
                (trends['end'] <= end) & (trends['end'] >= start)
                ]
            if len(start_overlaps) > 0:
                if len(start_overlaps) > 1:
                    raise Exception(f"{bull_bear_side_repr(start, trend)} have more than one start overlaps:"
                                    f"{[bull_bear_side_repr(o_start, o_trend) for o_start, o_trend in start_overlaps]}")
                if start != start_overlaps.iloc[0]['end']:
                    raise Exception(f"start of {bull_bear_side_repr(start, trend)} does not match with end of "
                                    f"{bull_bear_side_repr(start_overlaps.index[0], start_overlaps.iloc[0])}")
            if len(end_overlaps) > 0:
                if len(end_overlaps) > 1:
                    raise Exception(f"{bull_bear_side_repr(start, trend)} have more than one end overlaps:"
                                    f"{[bull_bear_side_repr(o_start, o_trend) for o_start, o_trend in end_overlaps]}")
                if trend['end'] != end_overlaps.index[0]:
                    raise Exception(f"end of {bull_bear_side_repr(start, trend)} does not match with start of "
                                    f"{bull_bear_side_repr(end_overlaps.index[0], end_overlaps.iloc[0])}")


def test_side_trend_not_overlapping_another_side(start: datetime, trend: pd.Series,
                                                 overlapping_trends: pt.DataFrame[BullBearSide],
                                                 raise_exception: bool = False):
    if TREND.SIDE.value in overlapping_trends['bull_bear_side'].unique():
        _message = f'{bull_bear_side_repr(start, trend)} overlaps with"'
        for o_start, o_trend in overlapping_trends[overlapping_trends['bull_bear_side'] == TREND.SIDE.value]:
            _message += bull_bear_side_repr(o_start, o_trend)
        if raise_exception:
            raise Exception(_message)
        else:
            log(_message)
            return False
    return True


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


# merged_trends = []
#
# for start, trend in trends_df.iterrows():
#     end = trend['end']
#     direction = trend['bull_bear_side']
#
#     # Find overlapping trends
#     overlaps = trends_df[
#         (trends_df.index <= end) & (trends_df['end'] >= start) & (trends_df['bull_bear_side'] == direction)
#         ]
#
#     # Check for reverse direction overlaps
#     reverse_direction = TREND.BULLISH.value if direction == TREND.BEARISH.value else TREND.BEARISH.value
#     reverse_overlaps = trends_df[
#         (trends_df.index <= end) & (trends_df['end'] >= start) & (trends_df['bull_bear_side'] == reverse_direction)
#         ]
#
#     # Check if any overlapping trend covers the current trend
#
#     if len(reverse_overlaps) > 0:
#         _message = f'{trend["bull_bear_side"].replace("_TREND", "")}: ' \
#                    f'{start.strftime("%H:%M")}-{trend["end"].strftime("%H:%M")}' \
#                    f" Reverse direction overlap with "
#         for r_start, r_trend in reverse_overlaps.iterrows():
#             _message += f'{r_trend["bull_bear_side"].replace("_TREND", "")}: ' \
#                         f'{r_start.strftime("%H:%M")}-{r_trend["end"].strftime("%H:%M")}'
#         log(_message)
#         # raise Exception(_message )
#
#     # Check if any BULL/BEAR trend covers SIDE trends
#     if direction == 'SIDE':
#         covering_bull_bear = overlaps[overlaps['bull_bear_side'].isin([TREND.BULLISH.value, TREND.BEARISH.value])]
#         if len(covering_bull_bear) > 0:
#             continue  # Skip this SIDE trend
#
#     # Merge overlapping trends
#     if len(overlaps) > 1:
#         end = max(overlaps['end'].max(), end)
#
#     merged_trends.append({'start': start, 'end': end, 'bull_bear_side': direction})
#
# merged_trends_df = pd.DataFrame(merged_trends)
# return merged_trends_df


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
