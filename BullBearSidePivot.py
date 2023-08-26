import os

import pandas as pd
from pandera import typing as pt

from BullBearSide import read_multi_timeframe_bull_bear_side_trends, previous_trend
from Candle import read_multi_timeframe_ohlca
from Config import config
from DataPreparation import single_timeframe, tolerance, to_timeframe
from FigurePlotter.Pivot_plotter import plot_multi_timeframe_bull_bear_side_pivots
from Model.MultiTimeframePivot import MultiTimeframePivot
from PeakValley import read_multi_timeframe_peaks_n_valleys, major_peaks_n_valleys, peaks_only, valleys_only
from helper import measure_time


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
    for timeframe in config.timeframes[::-1]:
        single_timeframe_peaks_n_valleys = major_peaks_n_valleys(multi_timeframe_peaks_n_valleys, timeframe)
        timeframe_ohlca = single_timeframe(multi_timeframe_ohlca, timeframe)
        timeframe_trends = single_timeframe(multi_timeframe_trends, timeframe)
        if len(timeframe_trends) > 0:
            timeframe_trends['previous_trend'], timeframe_trends['previous_trend_movement'] = previous_trend(timeframe_trends)
            # timeframe_trends.loc[timeframe_trends.index[1:], 'previous_trend_movement'] = \
            #     timeframe_trends.loc[timeframe_trends['previous_trend'].notna(), 'movement']
            # timeframe_trends.loc[timeframe_trends['previous_trend'].notna(), 'previous_trend_movement'] = \
            #     timeframe_trends.loc[timeframe_trends['previous_trend'].notna(), 'movement']
            _pivot_trends = timeframe_trends[
                (timeframe_trends['movement'] > tolerance(timeframe_trends['ATR']))
                & (timeframe_trends['previous_trend_movement'] > tolerance(timeframe_trends['ATR']) * 3)
                ]
            _pivot_top_indexes = timeframe_trends.loc[
                (timeframe_trends['movement'] > tolerance(timeframe_trends['ATR']))
                & (timeframe_trends['previous_trend_movement'] > tolerance(timeframe_trends['ATR']) * 3)
                , 'movement_start_time'
            ].unique()

            # timeframe_pivot_indexes = timeframe_trends[
            #     (timeframe_trends['movement'] > tolerance(timeframe_trends['ATR']))
            #     & (timeframe_trends['previous_trend_movement'] > tolerance(timeframe_trends['ATR']) * 3)
            #     ].index.get_level_values('date').unique()
            if len(_pivot_top_indexes) > 0:
                _pivots = pd.DataFrame(data={'date': _pivot_top_indexes}).set_index('date')
                # _pivots = pd.DataFrame(
                #     index=timeframe_trends.loc[timeframe_pivot_indexes, 'movement_start_time'])
                _pivots['activation_time'] = _pivots.index
                _pivots['movement_start_time'] = \
                    timeframe_trends.loc[_pivot_trends['previous_trend'], 'movement_start_time'].to_list()
                _pivots['movement_start_value'] = \
                    timeframe_trends.loc[_pivot_trends['previous_trend'], 'movement_start_value'].to_list()
                _pivots['return_end_time'] = _pivot_trends['movement_end_time'].to_list()
                _pivots['return_end_value'] = _pivot_trends['movement_end_value'].to_list()

                pivot_peaks_and_valleys = single_timeframe_peaks_n_valleys.loc[
                    single_timeframe_peaks_n_valleys.index.get_level_values('date').isin(_pivots.index)]
                # pivot_peaks_and_valleys.reset_index('timeframe', inplace=True)
                # pivot_peaks_and_valleys.drop(columns=['timeframe'])

                pivot_peaks = peaks_only(pivot_peaks_and_valleys)
                if len(pivot_peaks) > 0:
                    pivot_peak_times = pivot_peaks.index.get_level_values('date')
                    _pivots.loc[pivot_peak_times, 'level'] = pivot_peaks_and_valleys['high'].tolist()
                    _pivots.loc[pivot_peak_times, 'nearest_body'] = \
                        pivot_peaks_and_valleys[['open', 'close']].max(axis='columns').tolist()
                    pivot_peak_times_mapped_to_timeframe = \
                        [to_timeframe(pivot_peak_time, timeframe) for pivot_peak_time in pivot_peak_times]
                    pivot_peak_atrs = timeframe_ohlca.loc[pivot_peak_times_mapped_to_timeframe, 'ATR'].tolist()
                    _pivots.loc[pivot_peak_times, 'ATR_margin'] = [
                        high - atr
                        for high, atr in zip(pivot_peaks_and_valleys['high'].tolist(), pivot_peak_atrs)
                    ]
                    _pivots.loc[pivot_peak_times, 'internal_margin'] = \
                        _pivots.loc[pivot_peak_times, ['nearest_body', 'ATR_margin']].min(
                            axis='columns').tolist()
                    _pivots.loc[pivot_peak_times, 'external_margin'] = \
                        _pivots.loc[pivot_peak_times, 'level'].add(pivot_peak_atrs).tolist()

                pivot_valleys = valleys_only(pivot_peaks_and_valleys)
                if len(pivot_valleys) > 0:
                    pivot_valley_times = pivot_valleys.index.get_level_values('date')
                    _pivots.loc[pivot_valley_times, 'level'] = pivot_peaks_and_valleys['low'].tolist()
                    _pivots.loc[pivot_valley_times, 'nearest_body'] = \
                        pivot_peaks_and_valleys[['open', 'close']].min(axis='columns').tolist()
                    pivot_valley_times_mapped_to_timeframe = \
                        [to_timeframe(pivot_valley_time, timeframe) for pivot_valley_time in pivot_valley_times]
                    pivot_valley_atrs = timeframe_ohlca.loc[pivot_valley_times_mapped_to_timeframe, 'ATR'].tolist()
                    _pivots.loc[pivot_valley_times, 'ATR_margin'] = \
                        (pivot_peaks_and_valleys['low'].add(pivot_valley_atrs).tolist())
                    _pivots.loc[pivot_valley_times, 'internal_margin'] = \
                        _pivots.loc[pivot_valley_times, ['nearest_body', 'ATR_margin']].max(
                            axis='columns').tolist()
                    _pivots.loc[pivot_valley_times, 'external_margin'] = [
                        level - atr
                        for level, atr in
                        zip(_pivots.loc[pivot_valley_times, 'level'].to_list(), pivot_valley_atrs)
                    ]

                _pivots['overlapped_with_major_timeframe'] = None
                if len(multi_timeframe_pivots) > 0:
                    for pivot_time, pivot in _pivots.iterrows():
                        indexes_of_overlapping_from_major_timeframes = \
                            multi_timeframe_pivots[multi_timeframe_pivots.index.get_level_values('date') == pivot_time]
                        most_major_timeframe = multi_timeframe_pivots[
                            multi_timeframe_pivots.index.get_level_values('date').isin(
                                indexes_of_overlapping_from_major_timeframes)
                            & multi_timeframe_pivots['overlapped_with_major_timeframe'].isnull()].index[0]
                        _pivots.loc[pivot_time, 'overlapped_with_major_timeframe'] = most_major_timeframe

                _pivots['timeframe'] = timeframe
                _pivots.set_index('timeframe', append=True, inplace=True)
                _pivots = _pivots.swaplevel()
                multi_timeframe_pivots = pd.concat([_pivots, multi_timeframe_pivots])

    """
    date: pt.Index[datetime]
    movement_start_time: pt.Series[datetime]
    movement_start_value: pt.Series[datetime]
    return_end_time: pt.Series[datetime]
    return_end_value: pt.Series[datetime]
    level: pt.Series[float]
    internal_margin: pt.Series[float]
    external_margin: pt.Series[float]
    is_active: pt.Series[bool]
    hit: pt.Series[int]
    overlapped_with_major_timeframe: pt.Series[bool]
    """
    MultiTimeframePivot.validate(multi_timeframe_pivots)
    return multi_timeframe_pivots


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
