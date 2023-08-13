import os
from datetime import datetime, timedelta
from typing import List

import pandas as pd
import pandera
import pandera.typing as pt
from pandas import Timestamp
from plotly import graph_objects as plgo

import PeaksValleys
from Config import TopTYPE, config, TREND
from DataPreparation import read_file, read_multi_timeframe_ohlc, single_timeframe, file_id, save_figure, \
    read_multi_timeframe_ohlca, to_timeframe, OHLCA
from FigurePlotters import plot_multiple_figures
from PeaksValleys import plot_peaks_n_valleys, peaks_only, valleys_only, read_multi_timeframe_peaks_n_valleys, \
    major_peaks_n_valleys
from helper import log


class Boundary(pandera.DataFrameModel):
    date: pt.Index[datetime]
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
    # Todo: Not tested!
    # any 5min top is major to 1min candles so following condition is not right!
    # if len(peaks_n_valleys['timeframe'].unique()) > 1:
    #     raise Exception('Expected the peaks and valleys be grouped and filtered to see the same value for all rows.')
    ohlc = insert_previous_n_next_top(TopTYPE.PEAK, single_timeframe_peaks_n_valleys, ohlc)
    ohlc = insert_previous_n_next_top(TopTYPE.VALLEY, single_timeframe_peaks_n_valleys, ohlc)
    return ohlc


# def insert_previous_n_next_top(top_type: TopTYPE, peaks_n_valleys, ohlc):
#     tops = peaks_n_valleys[peaks_n_valleys['peak_or_valley'] == top_type.value]
#     high_or_low = 'high' if top_type == TopTYPE.PEAK else 'low'
#     for i in range(len(tops)):
# if i == len(tops) - 1:
#     for j in ohlc.loc[tops.index[i] < ohlc.index].index:
#         ohlc.at[j, f'previous_{top_type.value}_index'] = tops.index[i]
#         ohlc.at[j, f'previous_{top_type.value}_value'] = tops.iloc[i][high_or_low]
# else:
#     for j in ohlc.loc[(tops.index[i] < ohlc.index) & (ohlc.index <= tops.index[i + 1])].index:
#         ohlc.at[j, f'previous_{top_type.value}_index'] = tops.index[i]
#         ohlc.at[j, f'previous_{top_type.value}_value'] = tops.iloc[i][high_or_low]
#     if i == 0:
#         for j in ohlc.loc[ohlc.index <= tops.index[i]].index:
#             ohlc.at[j, f'next_{top_type.value}_index'] = tops.index[i]
#             ohlc.at[j, f'next_{top_type.value}_value'] = tops.iloc[i][high_or_low]
#     else:
#         for j in ohlc.loc[(tops.index[i - 1] < ohlc.index) & (ohlc.index <= tops.index[i])].index:
#             ohlc.at[j, f'next_{top_type.value}_index'] = tops.index[i]
#             ohlc.at[j, f'next_{top_type.value}_value'] = tops.iloc[i][high_or_low]
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


# def single_timeframe_candles_trend(ohlc: pd.DataFrame, single_timeframe_peaks_n_valley: pd.DataFrame) -> pd.DataFrame:
#     # Todo: Not tested!
#     # if timeframe not in config.timeframes:
#     #     raise Exception(f'Unsupported timeframe:{timeframe} expected to be from: [{config.timeframes}]')
#     # _higher_or_eq_timeframe_peaks_n_valleys = higher_or_eq_timeframe_peaks_n_valleys(peaks_n_valley, timeframe)
#     # if any([i not in ohlc.columns for i in
#     #         [f'{j}_{k}' for j in ['previous', 'next'] for k in [e.value for e in TopTYPE]]]):
#     #     ohlc = insert_previous_n_next_peaks_n_valleys(_higher_or_eq_timeframe_peaks_n_valleys, ohlc)
#     # ohlc[f'bull_bear_side_{timeframe}'] = TREND.SIDE
#     # ohlc.loc[
#     #     (ohlc['next_valley_value'] > ohlc['previous_valley_value']) &
#     #     (ohlc['next_peak_value'] > ohlc['previous_peak_value']) &
#     #     (ohlc['next_peak_index'] > ohlc['next_valley_index'])  # the higher peak should be after higher valley
#     #     , f'bull_bear_side_{timeframe}'] = TREND.BULLISH
#     # ohlc.loc[
#     #     (ohlc['next_peak_value'] < ohlc['previous_peak_value']) &
#     #     (ohlc['next_valley_value'] < ohlc['previous_valley_value']) &
#     #     (ohlc['next_peak_index'] > ohlc['next_valley_index'])  # the lower valley should be after lower peak
#     #     , f'bull_bear_side_{timeframe}'] = TREND.BEARISH
#     # return ohlc
#     # if timeframe not in config.timeframes:
#     #     raise Exception(f'Unsupported timeframe:{timeframe} expected to be from: [{config.timeframes}]')
#     candle_trend = insert_previous_n_next_tops(single_timeframe_peaks_n_valley, ohlc)
#     candle_trend['bull_bear_side'] = TREND.SIDE.value
#     # candle_trend = pd.DataFrame([TREND.SIDE.value] * len(ohlc_with_previous_n_next_tops),
#     #                             index=ohlc_with_previous_n_next_tops.index,
#     #                             columns=['bull_bear_side'])
#     candle_trend.loc[candle_trend.index[
#         (candle_trend['next_valley_value'] > candle_trend[
#             'previous_valley_value']) &
#         (candle_trend['next_peak_value'] > candle_trend['previous_peak_value']) &
#         (candle_trend['next_peak_index'] > candle_trend[
#             'next_valley_index'])]  # the higher peak should be after higher valley
#     , 'bull_bear_side'] = TREND.BULLISH.value
#     candle_trend.loc[candle_trend.index[
#         (candle_trend['next_peak_value'] < candle_trend['previous_peak_value']) &
#         (candle_trend['next_valley_value'] < candle_trend[
#             'previous_valley_value']) &
#         (candle_trend['next_peak_index'] > candle_trend[
#             'next_valley_index'])]  # the lower valley should be after lower peak
#     , 'bull_bear_side'] = TREND.BEARISH.value
#     return candle_trend
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


# def multi_timeframe_trend_boundaries(multi_timeframe_candle_trend: pd.DataFrame,
#                                      multi_timeframe_peaks_n_valleys: pd.DataFrame,
#                                      timeframe_shortlist: List['str'] = None):
# self.multi_timeframe_trend_boundaries_columns = ['timeframe', 'end', 'bull_bear_side',
#                                                  'highest_hig', 'lowest_low', 'high_time', 'low_time',
#                                                  'trend_line_acceleration', 'trend_line_base',
#                                                  'canal_line_acceleration', 'canal_line_base',
#                                                  ]
# timeframes = [i.replace('bull_bear_side_', '') for i in multi_timeframe_candle_trend.columns if
#                    i.startswith('bull_bear_side_')]
# for _, timeframe in enumerate(config.structure_timeframes):
#     if f'previous_bull_bear_side_{timeframe}' not in multi_timeframe_candle_trend.columns:
#         raise Exception(
#             f'previous_bull_bear_side_{timeframe} not found in candle_trend:({multi_timeframe_candle_trend.columns})')
# boundaries = pd.DataFrame()
# multi_timeframe_candle_trend['time_of_previous'] = multi_timeframe_candle_trend.index.shift(-1, freq='1min')
# for timeframe in timeframes:
#     zztodo: move effective time to a column to prevent multiple columns for different effective times.
#     multi_timeframe_candle_trend.loc[timeframe, 'previous_bull_bear_side'] = multi_timeframe_candle_trend[
#         f'bull_bear_side_{timeframe}'].shift(1)
#     time_boundaries = multi_timeframe_candle_trend[
#         multi_timeframe_candle_trend[f'previous_bull_bear_side_{timeframe}'] != multi_timeframe_candle_trend[
#             f'bull_bear_side_{timeframe}']]
#     time_boundaries['timeframe'] = timeframe
#     time_boundaries['bull_bear_side'] = time_boundaries[f'bull_bear_side_{timeframe}']
#     unnecessary_columns = [i for i in multi_timeframe_candle_trend.columns if
#                            i.startswith('bull_bear_side_') or i.startswith('previous_bull_bear_side_')]
#     time_boundaries.drop(columns=unnecessary_columns, inplace=True)
#     time_of_last_candle = multi_timeframe_candle_trend.index[-1]
#     time_boundaries.loc[:, 'time_of_next'] = time_boundaries.index
#     time_boundaries.loc[:, 'time_of_next'] = time_boundaries['time_of_next'].shift(-1)
#     time_boundaries.loc[time_boundaries.index[-1], 'time_of_next'] = time_of_last_candle
#     time_boundaries.loc[:, 'end'] = multi_timeframe_candle_trend.loc[
#         time_boundaries['time_of_next'], 'time_of_previous'].tolist()
#     time_boundaries.drop(columns=['time_of_next'])
#     boundaries = pd.concat([boundaries, time_boundaries])

def add_previous_toward_trend_top_to_boundary(single_timeframe_boundaries: pt.DataFrame[Boundary],
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


def zz_next_top_of_boundary(boundary, single_timeframe_peaks_n_valleys, top_type: TopTYPE):
    next_peak_after_boundary = single_timeframe_peaks_n_valleys[
        (single_timeframe_peaks_n_valleys.index.get_level_values('date') > boundary.end) &
        (single_timeframe_peaks_n_valleys.peak_or_valley == top_type.value)
        ].head(1)
    last_peak_inside_boundary = single_timeframe_peaks_n_valleys[
        (single_timeframe_peaks_n_valleys.index.get_level_values('date') <= boundary.end) &
        (single_timeframe_peaks_n_valleys.peak_or_valley == top_type.value)
        ].tail(1)
    return last_peak_inside_boundary, next_peak_after_boundary


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
        _timeframe_trend_boundaries = single_timeframe_bull_bear_side_trends(single_timeframe_candle_trend,
                                                                             single_timeframe_peaks_n_valleys,
                                                                             single_timeframe(multi_timeframe_ohlca,
                                                                                         timeframe)
                                                                             , timeframe)
        _timeframe_trend_boundaries['timeframe'] = timeframe
        _timeframe_trend_boundaries.set_index('timeframe', append=True, inplace=True)
        _timeframe_trend_boundaries = _timeframe_trend_boundaries.swaplevel()
        boundaries = pd.concat([boundaries, _timeframe_trend_boundaries])
    boundaries = boundaries[
        [column for column in config.multi_timeframe_trend_boundaries_columns if column != 'timeframe']]

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


def trend_ATRs(single_timeframe_boundaries: pt.DataFrame[Boundary], ohlca: pt.DataFrame[OHLCA]):
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


def trend_rate(_boundaries: pt.DataFrame[Boundary], timeframe: str) -> pt.DataFrame[Boundary]:
    return _boundaries['movement'] / (_boundaries['duration'] / pd.to_timedelta(timeframe))


def trend_duration(_boundaries: pt.DataFrame[Boundary]) -> pt.Series[timedelta]:
    durations = _boundaries['end'] - _boundaries.index
    for index, duration in durations.items():
        if not duration > timedelta(0):
            raise Exception(f'Duration must be greater than zero. But @{index}={duration}. in: {_boundaries}')
    return _boundaries['end'] - _boundaries.index


def ignore_weak_trend(_boundaries: pt.DataFrame[Boundary]) -> pt.DataFrame[Boundary]:
    """
    Remove weak trends from the DataFrame.

    Parameters:
        _boundaries (pt.DataFrame[Boundary]): A DataFrame containing trend boundary data.

    Returns:
        pt.DataFrame[Boundary]: A DataFrame with weak trends removed.

    Example:
        # Assuming you have a DataFrame '_boundaries' with the required columns and data
        filtered_boundaries = ignore_weak_trend(_boundaries)
    """
    # weak_boundaries = _boundaries.loc[filter_weak_trends(_boundaries) & (_boundaries['bull_bear_side'] != TREND.SIDE.value), 'bull_bear_side']
    # weak_boundaries = _boundaries.loc[filter_weak_trends(_boundaries), 'bull_bear_side']
    # filtered_boundaries = _boundaries.loc[_boundaries['strength'] < (
    #             _boundaries['ATR'] * config.momentum_trand_strength_factor), 'bull_bear_side']
    _boundaries.loc[_boundaries['strength'] < config.momentum_trand_strength_factor, 'bull_bear_side'] \
        = TREND.SIDE.value
    return _boundaries


def trend_movement(_boundaries: pt.DataFrame[Boundary]) -> pt.Series[float]:
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


def test_boundary_ATR(_boundaries: pt.DataFrame[Boundary]) -> bool:
    if _boundaries['ATR'].isnull().any():
        raise Exception(f'Nan ATR in: {_boundaries[_boundaries["ATR"].isna()]}')
    return True


def single_timeframe_bull_bear_side_trends(single_timeframe_candle_trend: pd.DataFrame,
                                           single_timeframe_peaks_n_valleys, ohlca: pt.DataFrame[OHLCA],
                                           timeframe: str) -> pd.DataFrame:
    single_timeframe_candle_trend = single_timeframe_candle_trend.loc[ohlca['ATR'].first_valid_index():]
    _boundaries = detect_boundaries(single_timeframe_candle_trend, timeframe)
    _boundaries = add_trend_tops(_boundaries, single_timeframe_candle_trend)

    _boundaries = add_previous_toward_trend_top_to_boundary(_boundaries, single_timeframe_peaks_n_valleys, timeframe)

    _boundaries['movement'] = trend_movement(_boundaries)
    _boundaries['ATR'] = trend_ATRs(_boundaries, ohlca=ohlca)
    test_boundary_ATR(_boundaries)
    _boundaries['duration'] = trend_duration(_boundaries)
    _boundaries['rate'] = trend_rate(_boundaries, timeframe)
    _boundaries['strength'] = trend_strength(_boundaries)
    back_boundaries = _boundaries.copy()
    # _boundaries = ignore_weak_trend(_boundaries)
    # todo: test merge_overlapped_trends
    _boundaries = merge_overlapped_trends(_boundaries)
    single_timeframe_ohlca = single_timeframe(read_multi_timeframe_ohlca(config.under_process_date_range), timeframe)
    plot_multiple_figures([
        plot_single_timeframe_trend_boundaries(single_timeframe_ohlca, single_timeframe_peaks_n_valleys,
                                               back_boundaries, name=f'back_boundaries', save=False, show=False),
        plot_single_timeframe_trend_boundaries(single_timeframe_ohlca, single_timeframe_peaks_n_valleys, _boundaries,
                                               name=f'modified_boundaries', save=False, show=False),
    ], name='compare after adding previous toward top')
    # _boundaries = add_canal_lines(_boundaries, single_timeframe_peaks_n_valleys)
    _boundaries = _boundaries[[i for i in config.multi_timeframe_trend_boundaries_columns if i != 'timeframe']]
    return _boundaries


def boundary_ATRs(_boundary_start: Timestamp, _boudary_end: Timestamp, ohlca: pt.DataFrame[OHLCA]) -> List[float]:
    return ohlca.loc[_boundary_start:_boudary_end, 'ATR']  # .fillna(0)


def detect_boundaries(single_timeframe_candle_trend, timeframe: str) -> pt.DataFrame[Boundary]:
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


def boundary_including_peaks_valleys(peaks_n_valleys: pd.DataFrame, boundary_start: pd.Timestamp,
                                     boundary_end: pd.Timestamp):
    return peaks_n_valleys.loc[(peaks_n_valleys.index.get_level_values('date') >= boundary_start) &
                               (peaks_n_valleys.index.get_level_values('date') <= boundary_end)]


MAX_NUMBER_OF_PLOT_SCATTERS = 50


def plot_single_timeframe_trend_boundaries(single_timeframe_ohlca: pd.DataFrame, peaks_n_valleys: pd.DataFrame,
                                           boundaries: pd.DataFrame,
                                           name: str = '', show: bool = True,
                                           html_path: str = '', save: bool = True) -> plgo.Figure:
    fig = plot_peaks_n_valleys(single_timeframe_ohlca, peaks=peaks_only(peaks_n_valleys),
                               valleys=valleys_only(peaks_n_valleys), file_name=name, show=False, save=False)
    remained_number_of_scatters = MAX_NUMBER_OF_PLOT_SCATTERS
    for boundary_index, boundary in boundaries.iterrows():
        if boundary_index == Timestamp('2017-01-04 11:17:00'):
            pass
        boundary_peaks_n_valleys = boundary_including_peaks_valleys(peaks_n_valleys, boundary_index,
                                                                    boundary['end'])
        boundary_indexes = single_timeframe_ohlca.loc[boundary_index: boundary['end']].index
        boundary_peaks = peaks_only(boundary_peaks_n_valleys)['high'].sort_index(level='date')
        boundary_valleys = valleys_only(boundary_peaks_n_valleys)['low'].sort_index(level='date', ascending=False)
        xs = [boundary_index] + boundary_peaks.index.get_level_values('date').tolist() + \
             [boundary['end']] + boundary_valleys.index.get_level_values('date').tolist()
        ys = [single_timeframe_ohlca.loc[boundary_index, 'open']] + boundary_peaks.values.tolist() + \
             [single_timeframe_ohlca.loc[boundary['end'], 'close']] + boundary_valleys.values.tolist()
        fill_color = 'green' if boundary['bull_bear_side'] == TREND.BULLISH.value else \
            'red' if boundary['bull_bear_side'] == TREND.BEARISH.value else 'gray'
        text = f'{boundary["bull_bear_side"].replace("_TREND")}: ' \
               f'{boundary_index.strftime("%H:%M")}-{boundary["end"].strftime("%H:%M")}:'
        if 'movement' in boundaries.columns.tolist():
            text += f'\nM:{boundary["movement"]:.2f}'
        if 'duration' in boundaries.columns.tolist():
            text += f'D:{boundary["duration"] / timedelta(hours=1):.2f}h'
        if 'strength' in boundaries.columns.tolist():
            text += f'S:{boundary["strength"]:.2f}'
        if 'ATR' in boundaries.columns.tolist():
            text += f'ATR:{boundary["ATR"]:.2f}'
        if remained_number_of_scatters > 0:
            fig.add_scatter(x=xs, y=ys, fill="toself",  # fillcolor=fill_color,
                            fillpattern=dict(fgopacity=0.5, shape='.'),
                            name=f'{boundary["bull_bear_side"].replace("_TREND")}: '
                                 f'{boundary_index.strftime("%H:%M")}-{boundary["end"].strftime("%H:%M")}',
                            line=dict(color=fill_color),
                            mode='lines',  # +text',
                            )
            fig.add_scatter(x=boundary_indexes, y=single_timeframe_ohlca.loc[boundary_indexes, 'open'],
                            mode='none',
                            showlegend=False,
                            text=text,
                            hoverinfo='text')

            remained_number_of_scatters -= 1
        else:
            break

    if save or html_path != '':
        file_name = f'single_timeframe_trend_boundaries.{file_id(single_timeframe_ohlca)}' if name == '' \
            else f'single_timeframe_trend_boundaries.{name}.{file_id(single_timeframe_ohlca)}'
        save_figure(fig, file_name, html_path)
    # if html_path == '':
    #     html_path = os.path.join(config.path_of_plots,
    #                              f'single_timeframe_trend_boundaries.{range_of_data(ohlc)}.html')
    # figure_as_html = fig.to_html()
    # with open(html_path, '+w') as f:
    #     f.write(figure_as_html)

    if show: fig.show()
    return fig


def read_multi_timeframe_trend_boundaries(date_range_str: str):
    return read_file(date_range_str, 'multi_timeframe_trend_boundaries', generate_multi_timeframe_bull_bear_side_trends)


def plot_multi_timeframe_trend_boundaries(multi_timeframe_ohlca, multi_timeframe_peaks_n_valleys,
                                          _multi_timeframe_trend_boundaries, show: bool = True, save: bool = True,
                                          timeframe_shortlist: List['str'] = None):
    figures = []
    if timeframe_shortlist is None:
        timeframe_shortlist = config.timeframes
    for timeframe in timeframe_shortlist:
        _figure = plot_single_timeframe_trend_boundaries(
            single_timeframe_ohlca=single_timeframe(multi_timeframe_ohlca, timeframe),
            peaks_n_valleys=major_peaks_n_valleys(multi_timeframe_peaks_n_valleys, timeframe).sort_index(level='date'),
            boundaries=single_timeframe(_multi_timeframe_trend_boundaries, timeframe).sort_index(level='date'),
            show=False, save=False,
            name=f'{timeframe} boundaries')
        figures.append(_figure)
    plot_multiple_figures(figures, name=f'multi_timeframe_trend_boundaries.'
                                        f'{multi_timeframe_ohlca.index[0][1].strftime("%y-%m-%d.%H-%M")}T'
                                        f'{multi_timeframe_ohlca.index[-1][1].strftime("%y-%m-%d.%H-%M")}',
                          show=show, save=save)


def generate_multi_timeframe_bull_bear_side_trends(date_range_str: str, file_path: str = config.path_of_data,
                                                   timeframe_short_list: List['str'] = None):
    multi_timeframe_ohlca = read_multi_timeframe_ohlca(date_range_str)
    multi_timeframe_peaks_n_valleys = read_multi_timeframe_peaks_n_valleys(date_range_str)
    # Generate multi-timeframe candle trend
    multi_timeframe_candle_trend = generate_multi_timeframe_candle_trend(date_range_str,
                                                                         timeframe_shortlist=timeframe_short_list)
    # Generate multi-timeframe trend boundaries
    trend_boundaries = multi_timeframe_bull_bear_side_trends(multi_timeframe_candle_trend,
                                                             multi_timeframe_peaks_n_valleys,
                                                             multi_timeframe_ohlca,
                                                             timeframe_shortlist=timeframe_short_list)
    # Plot multi-timeframe trend boundaries
    plot_multi_timeframe_trend_boundaries(multi_timeframe_ohlca, multi_timeframe_peaks_n_valleys, trend_boundaries,
                                          timeframe_shortlist=timeframe_short_list)
    # Save multi-timeframe trend boundaries to a.zip file
    trend_boundaries.to_csv(os.path.join(file_path, f'multi_timeframe_trend_boundaries.{date_range_str}.zip'),
                            compression='zip')


# def read_multi_timeframe_candle_trend(date_range_str: str):
#     return read_file(date_range_str, 'multi_timeframe_candle_trend', generate_multi_timeframe_candle_trend)

def plot_single_timeframe_candle_trend(ohlc: pd.DataFrame, single_timeframe_candle_trend: pd.DataFrame,
                                       single_timeframe_peaks_n_valleys: pd.DataFrame, show=True, save=True,
                                       path_of_plot=config.path_of_plots, name='Single Timeframe Candle Trend'):
    """
    Plot candlesticks with highlighted trends (Bullish, Bearish, Side).

    The function uses the provided DataFrame containing candle trends and highlights the bars of candles based on
    their trend. Bullish candles are displayed with 70% transparent green color, Bearish candles with 70% transparent red,
    and Side candles with 70% transparent grey color.

    Parameters:
        ohlc (pd.DataFrame): DataFrame containing OHLC data.
        single_timeframe_candle_trend (pd.DataFrame): DataFrame containing candle trend data.
        single_timeframe_peaks_n_valleys (pd.DataFrame): DataFrame containing peaks and valleys data.
        show (bool): If True, the plot will be displayed.
        save (bool): If True, the plot will be saved as an HTML file.
        path_of_plot (str): Path to save the plot.
        name (str): The title of the figure.

    Returns:
        plgo.Figure: The Plotly figure object containing the plot with highlighted trends.
    """
    # Calculate the trend colors
    trend_colors = single_timeframe_candle_trend['bull_bear_side'].map({
        TREND.BULLISH.value: 'rgba(0, 128, 0, 0.7)',  # 70% transparent green for Bullish trend
        TREND.BEARISH.value: 'rgba(255, 0, 0, 0.7)',  # 70% transparent red for Bearish trend
        TREND.SIDE.value: 'rgba(128, 128, 128, 0.7)'  # 70% transparent grey for Side trend
    })

    # Create the figure using plot_peaks_n_valleys function
    fig = plot_peaks_n_valleys(ohlc,
                               peaks=peaks_only(single_timeframe_peaks_n_valleys),
                               valleys=valleys_only(single_timeframe_peaks_n_valleys),
                               file_name=f'{name} Peaks n Valleys')

    # Update the bar trace with trend colors
    fig.update_traces(marker=dict(color=trend_colors), selector=dict(type='bar'))

    # Set the title of the figure
    fig.update_layout(title_text=name)

    # Show the figure or write it to an HTML file
    if save: save_figure(fig, name, file_id(ohlc))
    if show: fig.show()

    return fig


def plot_multi_timeframe_candle_trend(multi_timeframe_candle_trend, multi_timeframe_peaks_n_valleys, ohlc, show=True,
                                      save=True, path_of_plot=config.path_of_plots):
    # todo: test plot_multi_timeframe_candle_trend
    figures = []
    _multi_timeframe_peaks = peaks_only(multi_timeframe_peaks_n_valleys)
    _multi_timeframe_valleys = valleys_only(multi_timeframe_peaks_n_valleys)
    for _, timeframe in enumerate(config.timeframes):
        figures.append(
            plot_single_timeframe_candle_trend(ohlc, single_timeframe(multi_timeframe_candle_trend, timeframe),
                                               major_peaks_n_valleys(multi_timeframe_peaks_n_valleys, timeframe),
                                               show=True,
                                               save=True,
                                               path_of_plot=path_of_plot, name=f'{timeframe} Candle Trend'))
    plot_multiple_figures(figures, name='multi_timeframe_candle_trend', show=show, save=save,
                          path_of_plot=path_of_plot)


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


def merge_overlapped_trends():
    """
    overlapped_trends = trend of the same kind with at lease one candle in common.
    if any trend covered by a trend of reverse direction, raise exception!
    if any SIDE trend covered by a BULL/BEAR trend, drop the SIDE trend.
    :return:
    """
    # todo: implement merge_overlapped_trends
    raise Exception('Not implemented')

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
