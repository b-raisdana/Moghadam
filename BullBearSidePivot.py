import os
from typing import List

import pandas as pd
from pandera import typing as pt

import helper
from BullBearSide import read_multi_timeframe_bull_bear_side_trends, previous_trend
from Candle import read_multi_timeframe_ohlca
from Config import config
from DataPreparation import single_timeframe, expected_movement_size, trigger_timeframe, read_file, \
    cast_and_validate, anti_pattern_timeframe
from FigurePlotter.Pivot_plotter import plot_multi_timeframe_pivots
from Model.BullBearSide import BullBearSide
from Model.MultiTimeframePivot import MultiTimeframePivot
from Model.Pivot import BullBearSidePivot
from PeakValley import read_multi_timeframe_peaks_n_valleys, major_peaks_n_valleys
from Pivots import level_ttl
from PivotsHelper import pivots_level_n_margins
from helper import measure_time


def remove_overlapping_trends(timeframe_trends: pt.DataFrame[BullBearSide]) -> pt.DataFrame[BullBearSide]:
    """
        Remove overlapping trends from a DataFrame by selecting the trend with the maximum 'movement' within each date group.

        Args:
            timeframe_trends (pd.DataFrame[BullBearSide]): A DataFrame containing trend data.

        Returns:
            pd.DataFrame[BullBearSide]: A DataFrame with overlapping trends removed.
        """
    # Group the DataFrame by 'date' and find the index of the row with the maximum 'movement' within each group
    max_movement_indices = timeframe_trends.groupby('movement_start_time')['movement'].idxmax()

    # Select the rows with the maximum 'movement' based on the indices
    deduplicated_trends = timeframe_trends.loc[max_movement_indices]

    return deduplicated_trends


def multi_timeframe_bull_bear_side_pivots(date_range_str: str = None, timeframe_shortlist: List['str'] = None) \
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
    if date_range_str is None:
        date_range_str = helper.under_process_date_range

    multi_timeframe_trends = read_multi_timeframe_bull_bear_side_trends(date_range_str)
    multi_timeframe_peaks_n_valleys = read_multi_timeframe_peaks_n_valleys(date_range_str)
    multi_timeframe_ohlca = read_multi_timeframe_ohlca(date_range_str)
    multi_timeframe_pivots = pd.DataFrame()
    if timeframe_shortlist is None:
        timeframe_shortlist = config.structure_timeframes[::-1]
    for timeframe in timeframe_shortlist:
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
            _pivot_trends = remove_overlapping_trends(_pivot_trends)
            # _pivot_trends = _pivot_trends.groupby('movement_start_time', group_keys=False).apply(
            #     lambda x: x.loc[x['movement_start_time'].idxmin()])
            if len(_pivot_trends) > 0:
                """
                start of a trend considered as a Pivot if:
                    the movement of trend is > _expected_movement_size
                    the movement of previous trend is > _expected_movement_size * 3
                """
                _pivots = (pd.DataFrame(data={'date': _pivot_trends['movement_start_time'], 'ttl': None, 'hit': 0})
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
                _pivots = pivots_level_n_margins(pivot_peaks_and_valleys, _pivots, timeframe, timeframe_ohlca,
                                                 trigger_timeframe_ohlca)

                _pivots['ttl'] = _pivots.index + level_ttl(timeframe)
                _pivots['deactivated_at'] = None
                _pivots['archived_at'] = None
                _pivots['is_overlap_of'] = None
                _pivots = cast_and_validate(_pivots, BullBearSidePivot)
                # _pivots['timeframe'] = timeframe
                _pivots['timeframe'] = anti_pattern_timeframe(timeframe)
                _pivots.set_index('timeframe', append=True, inplace=True)
                _pivots = _pivots.swaplevel()
                multi_timeframe_pivots = pd.concat([_pivots, multi_timeframe_pivots])
    multi_timeframe_pivots = cast_and_validate(multi_timeframe_pivots, MultiTimeframePivot)
    return multi_timeframe_pivots


# def multi_timeframe_bull_bear_side_pivots(date_range_str: str = None, timeframe_shortlist: List['str'] = None) \
#         -> pt.DataFrame[MultiTimeframePivot]:
#     """
#     highest high of every Bullish and lowest low of every Bearish trend. for Trends
#             conditions:
#                 >= 3 ATR
#                 >= 1 ATR reverse movement after the most significant top before a trend with the same direction.
#                 if a trend with the same direction before < 1 ATR return found, merge these together.
#             index = time of pivot candle (highest high for Bullish and lowest low for Bearish) mapped to timeframe
#             exact_time = exact time of pivot candle
#             timeframe = time frame of pivot candle
#             value = highest high for Bullish and lowest low for Bearish
#             inner_margin = [Bullish: high - ]/[Bearish: low +]
#                 max(distance from nearest body of pivot and adjacent candles, 1 ATR in pivot timeframe)
#             outer_margin =
#     warning: if highest high is not the last peak of Bullish and lowest low is not the last Valley raise a warning log:
#                 timeframe, trend start time (index), time of last top
#                 time and high of highest high in Bullish and time and low of lowest low in Bearish,
#     :return:
#     """
#     if date_range_str is None:
#         date_range_str = config.under_process_date_range
#
#     multi_timeframe_trends = read_multi_timeframe_bull_bear_side_trends(date_range_str)
#     multi_timeframe_peaks_n_valleys = read_multi_timeframe_peaks_n_valleys(date_range_str)
#     multi_timeframe_ohlca = read_multi_timeframe_ohlca(date_range_str)
#     multi_timeframe_pivots = pd.DataFrame()
#     if timeframe_shortlist is None:
#         timeframe_shortlist = config.structure_timeframes[::-1]
#     for timeframe in timeframe_shortlist:
#         single_timeframe_peaks_n_valleys = major_peaks_n_valleys(multi_timeframe_peaks_n_valleys, timeframe)
#         timeframe_ohlca = single_timeframe(multi_timeframe_ohlca, timeframe)
#         trigger_timeframe_ohlca = single_timeframe(multi_timeframe_ohlca, trigger_timeframe(timeframe))
#         timeframe_trends = single_timeframe(multi_timeframe_trends, timeframe)
#         _expected_movement_size = expected_movement_size(timeframe_trends['ATR'])
#         if len(timeframe_trends) > 0:
#             timeframe_trends['previous_trend'], timeframe_trends['previous_trend_movement'] = previous_trend(
#                 timeframe_trends)
#             _pivot_trends = timeframe_trends[
#                 (timeframe_trends['movement'] > _expected_movement_size)
#                 & (timeframe_trends['previous_trend_movement'] > _expected_movement_size * 3)
#                 ]
#             timeframe_trends = remove_overlapping_trends(timeframe_trends)
#             # _pivot_trends = _pivot_trends.groupby('movement_start_time', group_keys=False).apply(
#             #     lambda x: x.loc[x['movement_start_time'].idxmin()])
#             if len(_pivot_trends) > 0:
#                 """
#                 start of a trend considered as a Pivot if:
#                     the movement of trend is > _expected_movement_size
#                     the movement of previous trend is > _expected_movement_size * 3
#                 """
#                 _pivot_indexes = timeframe_trends.loc[
#                     (timeframe_trends['movement'] > _expected_movement_size)
#                     & (timeframe_trends['previous_trend_movement'] > _expected_movement_size * 3)
#                     , 'movement_start_time'
#                 ].unique()
#                 _pivots = (pd.DataFrame(data={'date': _pivot_indexes, 'ttl': None, 'hit': 0})
#                            .set_index('date'))
#                 _pivots['activation_time'] = _pivots.index
#                 _pivots['movement_start_time'] = \
#                     timeframe_trends.loc[_pivot_trends['previous_trend'], 'movement_start_time'].to_list()
#                 _pivots['movement_start_value'] = \
#                     timeframe_trends.loc[_pivot_trends['previous_trend'], 'movement_start_value'].to_list()
#                 _pivots['return_end_time'] = _pivot_trends['movement_end_time'].to_list()
#                 _pivots['return_end_value'] = _pivot_trends['movement_end_value'].to_list()
#
#                 # find the Peaks and Valleys align with the Pivot
#                 pivot_peaks_and_valleys = single_timeframe_peaks_n_valleys.loc[
#                     single_timeframe_peaks_n_valleys.index.get_level_values('date').isin(_pivots.index)]
#                 _pivots = pivots_level_n_margins(pivot_peaks_and_valleys, _pivots, timeframe, trigger_timeframe_ohlca,
#                                                  trigger_timeframe_ohlca)
#
#                 _pivots['ttl'] = _pivots.index + level_ttl(timeframe)
#                 _pivots['deactivated_at'] = None
#                 _pivots['archived_at'] = None
#                 _pivots['is_overlap_of'] = None
#                 _pivots = cast_and_validate(_pivots, BullBearSidePivot)
#                 # _pivots['timeframe'] = timeframe
#                 _pivots['timeframe'] = anti_pattern_timeframe(timeframe)
#                 _pivots.set_index('timeframe', append=True, inplace=True)
#                 _pivots = _pivots.swaplevel()
#                 multi_timeframe_pivots = pd.concat([_pivots, multi_timeframe_pivots])
#     multi_timeframe_pivots = cast_and_validate(multi_timeframe_pivots, MultiTimeframePivot)
#     return multi_timeframe_pivots


def read_multi_timeframe_bull_bear_side_pivots(date_range_str: str = None) \
        -> pt.DataFrame[MultiTimeframePivot]:
    result = read_file(date_range_str, 'multi_timeframe_bull_bear_side_pivots',
                       generate_multi_timeframe_bull_bear_side_pivots < MultiTimeframePivot)
    return result


@measure_time
def generate_multi_timeframe_bull_bear_side_pivots(date_range_str: str = None,
                                                   file_path: str = config.path_of_data,
                                                   timeframe_shortlist: List['str'] = None):
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
    if date_range_str is None:
        date_range_str = helper.under_process_date_range
    multi_timeframe_pivots = multi_timeframe_bull_bear_side_pivots(date_range_str, timeframe_shortlist)
    # plot_multi_timeframe_pivots(multi_timeframe_pivots, name='multi_timeframe_bull_bear_side_pivots')
    multi_timeframe_pivots.to_csv(
        os.path.join(file_path, f'multi_timeframe_bull_bear_side_pivots.{date_range_str}.zip'),
        compression='zip')

    """
        in all boundaries with movement >= 1 ATR:
            if 
                movement of previous boundary >= 3 ATR
                or distance of most significant peak to reverse high/low of boundary >= 1 ATR 
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
    # raise Exception('Not implemented')
