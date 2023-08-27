import os

import numpy as np
import pandas as pd
from pandera import typing as pt

from BullBearSide import read_multi_timeframe_bull_bear_side_trends, previous_trend
from Candle import read_multi_timeframe_ohlca
from Config import config, TopTYPE
from DataPreparation import single_timeframe, tolerance, to_timeframe
from FigurePlotter.Pivot_plotter import plot_multi_timeframe_bull_bear_side_pivots
from Model.MultiTimeframePivot import MultiTimeframePivot
from Model.Pivot import Pivot
from PeakValley import read_multi_timeframe_peaks_n_valleys, major_peaks_n_valleys, peaks_only, valleys_only
from helper import measure_time, log


def pivots_level_n_margins(pivot_peaks_or_valleys, _type: TopTYPE, _pivots, timeframe, timeframe_ohlca) \
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

    if len(pivot_peaks_or_valleys) == 0:
        return _pivots

    if _type.value == 'peak':
        level_key = 'high'
        choose_body_operator = max
        internal_func = min
    else:  # 'valley'
        level_key = 'low'
        choose_body_operator = min
        internal_func = max

    pivot_times = pivot_peaks_or_valleys.index.get_level_values('date')
    _pivots.loc[pivot_times, 'level'] = pivot_peaks_or_valleys[level_key].tolist()
    pivot_times_mapped_to_timeframe = [to_timeframe(pivot_time, timeframe) for pivot_time in pivot_times]

    if _type.value == TopTYPE.PEAK.value:
        _pivots.loc[pivot_times, 'nearest_body'] = \
            pivot_peaks_or_valleys[['open', 'close']].apply(choose_body_operator, axis='columns').tolist()
    else:
        _pivots.loc[pivot_times, 'nearest_body'] = timeframe_ohlca.loc[
            pivot_times_mapped_to_timeframe, ['open', 'close']] \
            .apply(choose_body_operator, axis='columns').tolist()

    pivots_atr = timeframe_ohlca.loc[pivot_times_mapped_to_timeframe, 'ATR'].tolist()

    if _type.value == TopTYPE.PEAK.value:
        _pivots.loc[pivot_times, 'ATR_margin'] = [level - atr for level, atr in
                                                  zip(pivot_peaks_or_valleys[level_key].tolist(), pivots_atr)]
    else:
        _pivots.loc[pivot_times, 'ATR_margin'] = pivot_peaks_or_valleys[level_key].add(pivots_atr).tolist()

    _pivots.loc[pivot_times, 'internal_margin'] = _pivots.loc[pivot_times, ['nearest_body', 'ATR_margin']].apply(
        internal_func, axis='columns').tolist()

    if _type.value == TopTYPE.PEAK.value:
        _pivots.loc[pivot_times, 'external_margin'] = _pivots.loc[pivot_times, 'level'].add(pivots_atr).tolist()
    else:
        _pivots.loc[pivot_times, 'external_margin'] = [level - atr for level, atr in
                                                       zip(_pivots.loc[pivot_times, 'level'].to_list(), pivots_atr)]
    return _pivots


def update_hits(multi_timeframe_pivots):
    # Todo: implement update_hits
    log('Todo: implement update_hits')
    multi_timeframe_pivots['hit'] = 0
    return multi_timeframe_pivots


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
        timeframe_trends = single_timeframe(multi_timeframe_trends, timeframe)
        if len(timeframe_trends) > 0:
            timeframe_trends['previous_trend'], timeframe_trends['previous_trend_movement'] = previous_trend(
                timeframe_trends)
            _pivot_trends = timeframe_trends[
                (timeframe_trends['movement'] > tolerance(timeframe_trends['ATR']))
                & (timeframe_trends['previous_trend_movement'] > tolerance(timeframe_trends['ATR']) * 3)
                ]
            _pivot_trends = _pivot_trends.groupby('movement_start_time', group_keys=False).apply(
                lambda x: x.loc[x['movement_start_time'].idxmax()])
            if len(_pivot_trends) > 0:
                _pivot_top_indexes = timeframe_trends.loc[
                    (timeframe_trends['movement'] > tolerance(timeframe_trends['ATR']))
                    & (timeframe_trends['previous_trend_movement'] > tolerance(timeframe_trends['ATR']) * 3)
                    , 'movement_start_time'
                ].unique()

                _pivots = (pd.DataFrame(data={'date': _pivot_top_indexes, 'deactivation_time': None, 'hit': None})
                           .set_index('date'))
                _pivots['activation_time'] = _pivots.index
                _pivots['movement_start_time'] = \
                    timeframe_trends.loc[_pivot_trends['previous_trend'], 'movement_start_time'].to_list()
                _pivots['movement_start_value'] = \
                    timeframe_trends.loc[_pivot_trends['previous_trend'], 'movement_start_value'].to_list()
                _pivots['return_end_time'] = _pivot_trends['movement_end_time'].to_list()
                _pivots['return_end_value'] = _pivot_trends['movement_end_value'].to_list()

                pivot_peaks_and_valleys = single_timeframe_peaks_n_valleys.loc[
                    single_timeframe_peaks_n_valleys.index.get_level_values('date').isin(_pivots.index)]

                pivot_peaks = peaks_only(pivot_peaks_and_valleys)
                _pivots = pivots_level_n_margins(pivot_peaks, TopTYPE.PEAK, _pivots, timeframe, timeframe_ohlca)

                pivot_valleys = valleys_only(pivot_peaks_and_valleys)
                _pivots = pivots_level_n_margins(pivot_valleys, TopTYPE.VALLEY, _pivots, timeframe, timeframe_ohlca)

                _pivots = find_major_pivot_overlap(_pivots, multi_timeframe_pivots)
                _pivots = _pivots.astype({
                    'movement_start_time': np.datetime64,
                    'return_end_time': np.datetime64,
                    'activation_time': np.datetime64,
                    'deactivation_time': np.datetime64,
                    # 'overlapped_with_major_timeframe': str,
                })
                Pivot.validate(_pivots)

                _pivots['timeframe'] = timeframe
                _pivots.set_index('timeframe', append=True, inplace=True)
                _pivots = _pivots.swaplevel()
                multi_timeframe_pivots = pd.concat([_pivots, multi_timeframe_pivots])
    multi_timeframe_pivots = update_hits(multi_timeframe_pivots)

    MultiTimeframePivot.validate(multi_timeframe_pivots)
    return multi_timeframe_pivots


def find_major_pivot_overlap(new_pivots, multi_timeframe_pivots):
    new_pivots['overlapped_with_major_timeframe'] = None
    if len(multi_timeframe_pivots) > 0:
        for pivot_time, pivot in new_pivots.iterrows():
            overlapping_major_timeframes = \
                multi_timeframe_pivots[multi_timeframe_pivots.index.get_level_values('date') == pivot_time]
            if len(overlapping_major_timeframes) > 0:
                root_pivot = overlapping_major_timeframes[
                    multi_timeframe_pivots['overlapped_with_major_timeframe'].isna()
                ]
                if len(root_pivot) == 1:
                    new_pivots.loc[pivot_time, 'overlapped_with_major_timeframe'] = \
                        root_pivot.index.get_level_values('timeframe')[0]
                else:
                    raise Exception(f'Expected to find only one root_pivot but found({len(root_pivot)}):{root_pivot}')
    return new_pivots


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
    plot_multi_timeframe_bull_bear_side_pivots(multi_timeframe_pivots)
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
    raise Exception('Not implemented')
