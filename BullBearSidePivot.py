import os

import numpy as np
import pandas as pd
from pandera import typing as pt

from BullBearSide import read_multi_timeframe_bull_bear_side_trends, previous_trend
from Candle import read_multi_timeframe_ohlca
from Config import config, TopTYPE
from DataPreparation import single_timeframe, expected_movement_size, to_timeframe, trigger_timeframe, read_file
from FigurePlotter.Pivot_plotter import plot_multi_timeframe_pivots
from Model.MultiTimeframePivot import MultiTimeframePivot
from Model.Pivot import Pivot
from PeakValley import read_multi_timeframe_peaks_n_valleys, major_peaks_n_valleys, peaks_only, valleys_only
from Pivots import pivot_exact_overlapped, level_ttl
from helper import measure_time, log


def single_timeframe_pivots_level_n_margins(single_timeframe_pivot_peaks_or_valleys, _type: TopTYPE, _pivots, timeframe,
                                            timeframe_ohlca, trigger_timeframe_ohlca) \
        -> pt.DataFrame[Pivot]:
    """
        Processes the pivot data to determine levels, margins, and other metrics.

        Parameters:
        - pivot_data (DataFrame): Input pivot data, typically containing high and low prices.
        - _type (TopTYPE): Enum indicating whether the pivot data represents peaks or valleys.
        - _pivots (DataFrame): DataFrame to store processed pivot data.
        - timeframe (str): A string specifying the desired timeframe for mapping pivot times.
                          Must exist in config.timeframe.
        - timeframe_ohlca (DataFrame): DataFrame containing 'open', 'high', 'low', 'close' and 'ATR' columns for specific timeframes.

        Returns:
        - DataFrame: Updated _pivots DataFrame with the processed pivot data.

        Raises:
        - ValueError: If an invalid _type is provided or if the timeframe is not valid.

        Notes:
        - This function assumes that the provided DataFrame columns and data types are consistent with typical OHLC financial data.
        """

    if _type.value not in ['peak', 'valley']:
        raise ValueError("Invalid type. Use either 'peak' or 'valley'.")

    if timeframe not in config.timeframes:
        raise ValueError(f"'{timeframe}' is not a valid timeframe. Please select from {config.timeframe}.")

    if len(single_timeframe_pivot_peaks_or_valleys) == 0:
        return _pivots

    if _type.value == 'peak':
        level_key = 'high'
    else:  # 'valley'
        level_key = 'low'

    pivot_times = single_timeframe_pivot_peaks_or_valleys.index.get_level_values('date')
    _pivots.loc[pivot_times, 'level'] = single_timeframe_pivot_peaks_or_valleys[level_key].tolist()

    _pivots = pivot_margins(_pivots, _type, single_timeframe_pivot_peaks_or_valleys, timeframe_ohlca, timeframe,
                            trigger_timeframe_ohlca)

    return _pivots


def pivot_margins(pivots, _type: TopTYPE, pivot_peaks_or_valleys, timeframe_ohlca, timeframe, trigger_timeframe_ohlca):
    if _type.value not in ['peak', 'valley']:
        raise ValueError("Invalid type. Use either 'peak' or 'valley'.")
    if _type.value == 'peak':
        level_key = 'high'
        choose_body_operator = max
        internal_func = min
    else:  # 'valley'
        level_key = 'low'
        choose_body_operator = min
        internal_func = max

    pivot_times = pivot_peaks_or_valleys.index.get_level_values('date')
    pivot_times_mapped_to_timeframe = [to_timeframe(pivot_time, timeframe) for pivot_time in pivot_times]

    if _type.value == TopTYPE.PEAK.value:
        pivots.loc[pivot_times, 'nearest_body'] = \
            pivot_peaks_or_valleys[['open', 'close']].apply(choose_body_operator, axis='columns').tolist()
    else:
        pivots.loc[pivot_times, 'nearest_body'] = timeframe_ohlca.loc[
            pivot_times_mapped_to_timeframe, ['open', 'close']] \
            .apply(choose_body_operator, axis='columns').tolist()

    pivots_atr = trigger_timeframe_ohlca.loc[pivot_times_mapped_to_timeframe, 'ATR'].tolist()

    if _type.value == TopTYPE.PEAK.value:
        pivots.loc[pivot_times, 'ATR_margin'] = [level - atr for level, atr in
                                                 zip(pivot_peaks_or_valleys[level_key].tolist(), pivots_atr)]
    else:
        pivots.loc[pivot_times, 'ATR_margin'] = pivot_peaks_or_valleys[level_key].add(pivots_atr).tolist()

    pivots.loc[pivot_times, 'internal_margin'] = pivots.loc[pivot_times, ['nearest_body', 'ATR_margin']].apply(
        internal_func, axis='columns').tolist()

    if _type.value == TopTYPE.PEAK.value:
        pivots.loc[pivot_times, 'external_margin'] = pivots.loc[pivot_times, 'level'].add(pivots_atr).tolist()
    else:
        pivots.loc[pivot_times, 'external_margin'] = \
            [level - atr for level, atr in zip(pivots['level'].to_list(), pivots_atr)]
    return pivots


def multi_timeframe_bull_bear_side_pivots(date_range_str: str = config.under_process_date_range) \
        -> pt.DataFrame[MultiTimeframePivot]:
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
    multi_timeframe_trends = read_multi_timeframe_bull_bear_side_trends(date_range_str)
    multi_timeframe_peaks_n_valleys = read_multi_timeframe_peaks_n_valleys(date_range_str)
    multi_timeframe_ohlca = read_multi_timeframe_ohlca(date_range_str)
    multi_timeframe_pivots = pd.DataFrame()
    for timeframe in config.structure_timeframes[::-1]:
        single_timeframe_peaks_n_valleys = major_peaks_n_valleys(multi_timeframe_peaks_n_valleys, timeframe)
        timeframe_ohlca = single_timeframe(multi_timeframe_ohlca, timeframe)
        trigger_timeframe_ohlca = single_timeframe(multi_timeframe_ohlca, trigger_timeframe(timeframe))
        timeframe_trends = single_timeframe(multi_timeframe_trends, timeframe)
        _expected_movement_size = expected_movement_size(timeframe_trends['ATR'])
        if len(timeframe_trends) > 0:
            timeframe_trends['previous_trend'], timeframe_trends['previous_trend_movement'] = previous_trend(
                timeframe_trends)
            _pivot_trends = timeframe_trends[
                (timeframe_trends['movement'] > _expected_movement_size)
                & (timeframe_trends['previous_trend_movement'] > _expected_movement_size * 3)
                ]
            _pivot_trends = _pivot_trends.groupby('movement_start_time', group_keys=False).apply(
                lambda x: x.loc[x['movement_start_time'].idxmax()])
            if len(_pivot_trends) > 0:
                """
                start of a trend considered as a Pivot if:
                    the movement of trend is > _expected_movement_size
                    the movement of previous trend is > _expected_movement_size * 3
                """

                # start of a trend considered as a Pivot if the movement of previous trend is > _expected_movement_size * 3
                _pivot_indexes = timeframe_trends.loc[
                    (timeframe_trends['movement'] > _expected_movement_size)
                    & (timeframe_trends['previous_trend_movement'] > _expected_movement_size * 3)
                    , 'movement_start_time'
                ].unique()

                _pivots = (pd.DataFrame(data={'date': _pivot_indexes, 'ttl': None, 'hit': 0})
                           .set_index('date'))
                _pivots['activation_time'] = _pivots.index
                _pivots['movement_start_time'] = \
                    timeframe_trends.loc[_pivot_trends['previous_trend'], 'movement_start_time'].to_list()
                _pivots['movement_start_value'] = \
                    timeframe_trends.loc[_pivot_trends['previous_trend'], 'movement_start_value'].to_list()
                _pivots['return_end_time'] = _pivot_trends['movement_end_time'].to_list()
                _pivots['return_end_value'] = _pivot_trends['movement_end_value'].to_list()

                # find the Peaks and Valleys align with the Pivot
                pivot_peaks_and_valleys = single_timeframe_peaks_n_valleys.loc[
                    single_timeframe_peaks_n_valleys.index.get_level_values('date').isin(_pivots.index)]

                pivot_peaks = peaks_only(pivot_peaks_and_valleys)
                _pivots = single_timeframe_pivots_level_n_margins(pivot_peaks, TopTYPE.PEAK, _pivots, timeframe,
                                                                  timeframe_ohlca
                                                                  , trigger_timeframe_ohlca)

                pivot_valleys = valleys_only(pivot_peaks_and_valleys)
                _pivots = single_timeframe_pivots_level_n_margins(pivot_valleys, TopTYPE.VALLEY, _pivots, timeframe,
                                                                  timeframe_ohlca
                                                                  , trigger_timeframe_ohlca)
                _pivots['ttl'] = _pivots.index + level_ttl(timeframe)
                _pivots['deactivated_at'] = None
                _pivots['archived_at'] = None
                _pivots['is_overlap_of'] = None
                _pivots = _pivots.astype({
                    'movement_start_time': np.datetime64,
                    'return_end_time': np.datetime64,
                    'activation_time': np.datetime64,
                    'ttl': np.datetime64,
                    'deactivated_at': np.datetime64,
                    'archived_at': np.datetime64,
                    # 'is_overlap_of': str,
                })
                Pivot.validate(_pivots)
                _pivots = _pivots[[column
                                   for column in Pivot.__fields__.keys() if column not in ['timeframe', 'date']]]
                _pivots['timeframe'] = timeframe
                _pivots.set_index('timeframe', append=True, inplace=True)
                _pivots = _pivots.swaplevel()
                multi_timeframe_pivots = pd.concat([_pivots, multi_timeframe_pivots])
    MultiTimeframePivot.validate(multi_timeframe_pivots)
    return multi_timeframe_pivots


def read_multi_timeframe_bull_bear_side_pivots(date_range_str: str = config.under_process_date_range) \
        -> pt.DataFrame[MultiTimeframePivot]:
    result = read_file(date_range_str, 'multi_timeframe_bull_bear_side_pivots',
                       generate_multi_timeframe_bull_bear_side_pivots< MultiTimeframePivot)
    return result


@measure_time
def generate_multi_timeframe_bull_bear_side_pivots(date_range_str: str = config.under_process_date_range,
                                                   file_path: str = config.path_of_data):
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
    multi_timeframe_pivots = multi_timeframe_bull_bear_side_pivots(date_range_str)
    plot_multi_timeframe_pivots(multi_timeframe_pivots, name='multi_timeframe_bull_bear_side_pivots')
    multi_timeframe_pivots.to_csv(
        os.path.join(file_path, f'multi_timeframe_bull_bear_side_pivots.{date_range_str}.zip'),
        compression='zip')

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
    log('Not finished?')
    # raise Exception('Not implemented')
