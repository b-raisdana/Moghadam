from typing import List

import pandas as pd
from pandera import typing as pt

from Config import config, TREND
from PanderaDFM.BasePattern import MultiTimeframeBasePattern
from PanderaDFM.BullBearSide import BullBearSide, MultiTimeframeBullBearSide
from PanderaDFM.OHLCV import OHLCV, MultiTimeframeOHLCV
from PanderaDFM.Pivot import PivotDFM
from PanderaDFM.Pivot2 import MultiTimeframePivot2DFM
from PeakValley import insert_crossing2
from helper.data_preparation import single_timeframe


def merge_bbs_overlap(time_frame_bbs: pt.DataFrame[BullBearSide]):
    """
    find time_frame_bbs which overlaps with any other time_frame_bbs by comparing time_frame_bbs['movement_start_time']
    and time_frame_bbs['movement_end_time']. the overlapping  time_frame_bbs s have movement_start_time of one before
    movement_end_time of another.
    find the nearest time_frame_bbs with movement_start_time after movement_start_time of the row using pd.merage_asof()
    and add the index as 'next_movement_start_time'.
    All time_frame_bbs with next_movement_start_time < time_frame_bbs['movement_end_time'] indicates an overlap.
    :param time_frame_bbs:
    :param remove:
    :return:
    """
    if len(time_frame_bbs) <= 1:
        return
    next_movements = time_frame_bbs.copy()[['movement_start_time', 'movement_end_time']] \
        .rename(columns={'movement_start_time': 'next_movement_start_time',
                         'movement_end_time': 'next_movement_end_time'})
    next_movements['shifted_movement_start_time'] = \
        next_movements['next_movement_start_time'] - pd.to_timedelta(config.timeframes[0])
    next_movements['next_movement_index'] = next_movements.index
    next_movements.sort_values(by='next_movement_start_time', inplace=True)
    time_frame_bbs['date_backup'] = time_frame_bbs.index
    # drop overlaps with the same start
    merged_to_find_same_start = \
        time_frame_bbs.merge(next_movements, left_on='movement_start_time', right_on='next_movement_start_time',
                             how='left')
    valid_same_start_overlaps = merged_to_find_same_start[
        merged_to_find_same_start['next_movement_index'].notna()
        & (merged_to_find_same_start['next_movement_index'] != merged_to_find_same_start.index)
        & (merged_to_find_same_start['next_movement_end_time'] < merged_to_find_same_start['movement_end_time'])
        ]
    time_frame_bbs.drop(labels=valid_same_start_overlaps['next_movement_index'].dropna().unique(),
                        inplace=True)

    # find overlaps which does not have the same start
    merged_to_find_overlap = \
        pd.merge_asof(left=time_frame_bbs.sort_values(by='movement_start_time'), right=next_movements,
                      left_on='movement_start_time', right_on='shifted_movement_start_time', direction='forward')
    overlapped_movements = merged_to_find_overlap[
        merged_to_find_overlap['next_movement_start_time'].notna()
        & (merged_to_find_overlap['next_movement_index'] != time_frame_bbs.index)
        & (merged_to_find_overlap['next_movement_start_time'] < merged_to_find_overlap['movement_end_time'])
        ]
    # time_frame_bbs.loc[invalid_overlap_movements.index, ['next_movement_start_time', 'next_movement_index']] = pd.NA

    # drop overlap_movements which does not need merging. These overlaps are inside the original trend boundaries.
    if any(overlapped_movements['next_movement_index'].isna()):
        raise AssertionError("any(overlapped_movements['next_movement_index'].isna())")
    if len(overlapped_movements) > 0:
        pass  # todo: test
    no_merge_overlap_movements = overlapped_movements[
        overlapped_movements['next_movement_end_time'] <= overlapped_movements['movement_end_time']
        ]
    if len(no_merge_overlap_movements) > 0:
        time_frame_bbs.drop(labels=no_merge_overlap_movements['next_movement_index'].dropna().unique(),
                            inplace=True)  # todo: test
    # expand the end of first overlapping movement, to the end of second one to cover both

    # to_merge_overlap_movements = time_frame_bbs[
    #     time_frame_bbs['next_movement_index'].notna()
    #     # & (time_frame_bbs['next_movement_end_time'] > time_frame_bbs['movement_end_time'])
    # ]
    to_merge_overlap_movements = overlapped_movements[
        overlapped_movements.index.difference(no_merge_overlap_movements.index)]
    if len(to_merge_overlap_movements) > 0:
        time_frame_bbs.loc[to_merge_overlap_movements.index, 'movement_end_time'] = \
            time_frame_bbs.loc[to_merge_overlap_movements.index, 'next_movement_end_time']  # todo: test
        # drop trends which covered by the original after expansion.
        time_frame_bbs.drop(
            labels=to_merge_overlap_movements['next_movement_index'].dropna().unique(), inplace=True)


def pivot_bull_bear_movement_start(timeframe_pivots: pt.DataFrame[PivotDFM],
                                   time_frame_bbs_trends: pt.DataFrame[BullBearSide]):
    """
    Find the time_frame_bbs_boundary which matches with each pivot:
    Remove overlapped boundaries to prevent finding multiple bbs_trends for each pivot.
    :param time_frame_bbs_trends:
    :param bull_bear_trends:
    :param timeframe_pivots:
    :param pivots:
    :param bbs_boundaries:
    :return:
    """
    bull_bear_trends = \
        time_frame_bbs_trends[time_frame_bbs_trends['bull_bear_side'].isin([TREND.BULLISH.value, TREND.BEARISH.value])]
    merge_bbs_overlap(time_frame_bbs=bull_bear_trends)
    bull_bear_trends = \
        bull_bear_trends.rename(columns={
            'movement_start_time': 'bbs_movement_start_time',
            'movement_start_value': 'bbs_movement_start_value',
        })[['bbs_movement_start_time', 'bbs_movement_start_value', ]]
    timeframe_pivots.loc[:, ['bbs_movement_start_time', 'bbs_movement_start_value']] = pd.merge_asof(
        left=timeframe_pivots, right=bull_bear_trends.dropna(), left_index=True,
        right_on='bbs_movement_start_time',
        direction='forward'
    )[['bbs_movement_start_time', 'bbs_movement_start_value']]


def ftc_of_range(pivots_with_ftc_range_start_time, multi_timeframe_base_patterns, pivots_timeframe):
    if len(pivots_with_ftc_range_start_time) > 0:
        pass  # todo: test
    pivot_lower_timeframes = config.timeframes[config.timeframes.index(pivots_timeframe):0:-1]
    remained_pivots = pivots_with_ftc_range_start_time
    for timeframe in pivot_lower_timeframes:
        if len(remained_pivots) > 0:
            timeframe_base_patterns = single_timeframe(multi_timeframe_base_patterns, timeframe)
            for pivot_start, pivot in remained_pivots.iterrows():
                remained_pivots.loc[pivot_start, 'ftc_list'] = timeframe_base_patterns[
                    (timeframe_base_patterns.index < pivot_start)
                    & (timeframe_base_patterns.index > pivot['ftc_range_start_time'])
                    ]
            remained_pivots = remained_pivots[remained_pivots['ftc_list'].isna()]
        else:
            break


def multi_timeframe_ftc(
        mt_pivot: pt.DataFrame[MultiTimeframePivot2DFM],
        mt_frame_bbs_trend: pt.DataFrame[MultiTimeframeBullBearSide],
        mt_ohlcv: pt.DataFrame[MultiTimeframeOHLCV],
        multi_timeframe_base_patterns: pt.DataFrame[MultiTimeframeBasePattern],
        timeframe_shortlist: List[str] = None):
    if timeframe_shortlist is None:
        timeframe_shortlist = config.structure_timeframes[::-1]
    else:  # filter and order
        timeframe_shortlist = [timeframe for timeframe in config.structure_timeframes[::-1]
                               if timeframe in timeframe_shortlist]
    timeframe_shortlist = [timeframe for timeframe in timeframe_shortlist
                           if timeframe in mt_pivot.index.get_level_values(level='timeframe')]
    for timeframe in timeframe_shortlist:
        timeframe_pivots = single_timeframe(mt_pivot, timeframe)
        timeframe_bbs = single_timeframe(mt_frame_bbs_trend, timeframe)
        ohlcv = single_timeframe(mt_ohlcv, timeframe)
        find_ftc(
            timeframe_pivots=timeframe_pivots,
            time_frame_bbs_boundaries=timeframe_bbs,
            ohlcv=ohlcv, multi_timeframe_base_patterns=multi_timeframe_base_patterns,
            timeframe=timeframe)


def find_ftc(timeframe_pivots: pt.DataFrame[PivotDFM], time_frame_bbs_boundaries: pt.DataFrame[BullBearSide],
             ohlcv: pt.DataFrame[OHLCV], multi_timeframe_base_patterns: pt.DataFrame[MultiTimeframeBasePattern],
             timeframe):
    # todo: after testing properly, extend the operation to support duplicated pivots.
    original_pivots = timeframe_pivots[
        timeframe_pivots.index.get_level_values(level='date')
        == timeframe_pivots.index.get_level_values(level='original_start')] \
        .copy().reset_index().set_index('date')
    pivot_bull_bear_movement_start(original_pivots, time_frame_bbs_boundaries)
    pivots_with_bbs_movement = original_pivots[original_pivots['bbs_movement_start_value'].notna()]
    if len(pivots_with_bbs_movement) > 0:
        pass  # todo: test
    pivots_with_bbs_movement['bbs_movement'] = \
        pivots_with_bbs_movement['bbs_movement_start_value'] - pivots_with_bbs_movement['level']
    pivots_with_bbs_movement['ftc_range_start_value'] = \
        pivots_with_bbs_movement['level'] - pivots_with_bbs_movement['bbs_movement'] * config.ftc_price_range_percentage
    ftc_range_start_time(pivots_with_bbs_movement, ohlcv)
    ftc_of_range(pivots_with_bbs_movement, multi_timeframe_base_patterns, timeframe)


def ftc_range_start_time(pivots_with_bbs_movement, ohlcv):
    if len(pivots_with_bbs_movement) > 0:
        pass
    resistance_pivots = pivots_with_bbs_movement[pivots_with_bbs_movement['is_resistance']]  # todo: test
    pivots_with_bbs_movement.loc[resistance_pivots.index, 'ftc_range_start_time'] = (
        insert_crossing2(base=resistance_pivots, base_target_column='ftc_range_start_value',
                         target=ohlcv, target_compare_column='low', direction='left',
                         more_significant=lambda target, base: target < base,
                         ))['left_crossing_time']
    support_pivots = pivots_with_bbs_movement[~pivots_with_bbs_movement['is_resistance']]
    pivots_with_bbs_movement.loc[support_pivots.index, 'ftc_range_start_time'] = (
        insert_crossing2(base=support_pivots, base_target_column='ftc_range_start_value',
                         target=ohlcv, target_compare_column='high', direction='left',
                         more_significant=lambda target, base: target > base,
                         ))['left_crossing_time']
