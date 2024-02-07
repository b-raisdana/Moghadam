from typing import List

import pandas as pd
from pandera import typing as pt

from Config import config
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
        .rename(columns={'movement_end_time': 'next_movement_end_time'})  # todo: test
    next_movements['shifted_movement_start_time'] = \
        next_movements['movement_start_time'].shift(-1, freq=config.timeframes[0])
    next_movements['next_movement_index'] = next_movements.index
    next_movements.sort_values(by='movement_start_time', inplace=True)
    # drop overlaps with the same start
    time_frame_bbs[['same_start_movement_index', 'same_start_movement_end_time']] = \
        time_frame_bbs.merge(next_movements, left_on='movement_start_time', right_on='movement_start_time', how='left') \
            [['next_movement_index', 'next_movement_end_time']]
    invalid_same_start_indexes = time_frame_bbs[
        time_frame_bbs['same_start_movement_index'].isna()
        | (time_frame_bbs['same_start_movement_index'] == time_frame_bbs.index)
        | (time_frame_bbs['same_start_movement_end_time'] < time_frame_bbs['movement_end_time'])
        ]
    time_frame_bbs.loc[
        invalid_same_start_indexes.index, ['same_start_movement_index', 'same_start_movement_end_time']] = pd.NA
    time_frame_bbs.drop(labels=time_frame_bbs['same_start_movement_index'].dropna().unique(), inplace=True)

    # find overlaps which does not have the same start
    time_frame_bbs[['next_movement_start_time', 'next_movement_index']] = \
        pd.merge_asof(left=time_frame_bbs.sort_values(by='movement_start_time'), right=next_movements,
                      left_on='movement_start_time', right_on='shifted_movement_start_time', direction='forward') \
            [['next_movement_start_time', 'next_movement_index']]
    invalid_overlap_movements = time_frame_bbs[
        time_frame_bbs['next_movement_start_time'].isna()
        | (time_frame_bbs['next_movement_index'] == time_frame_bbs.index)
        | (time_frame_bbs['next_movement_start_time'] >= time_frame_bbs['movement_end'])
        ]
    time_frame_bbs.loc[invalid_overlap_movements.index, ['next_movement_start_time', 'next_movement_index']] = pd.NA
    # drop overlap_movements which does not need merging. These overlaps are inside the original trend boundaries.
    no_merge_overlap_movements = time_frame_bbs[
        time_frame_bbs['next_movement_index'].notna()
        & ~(time_frame_bbs['next_movement_end_time'] > time_frame_bbs['movement_end_time'])
        ]
    time_frame_bbs.drop(labels=no_merge_overlap_movements.index)
    # expand merge trend to cover overlap_movements which need merging.
    # These overlaps start after original trend start and end after original trends end.
    to_merge_overlap_movements = time_frame_bbs[
        time_frame_bbs['next_movement_index'].notna()
        # & (time_frame_bbs['next_movement_end_time'] > time_frame_bbs['movement_end_time'])
    ]
    time_frame_bbs.loc[to_merge_overlap_movements, 'movement_end_time'] = \
        time_frame_bbs.loc[to_merge_overlap_movements, 'next_movement_end_time']
    # drop trends which covered by the original after expansion.
    time_frame_bbs.drop(
        labels=time_frame_bbs.loc[to_merge_overlap_movements, 'next_movement_index'].dropna().unique(), inplace=True)


def pivot_bbs_movement_start(timeframe_pivots: pt.DataFrame[PivotDFM],
                             time_frame_bbs_trends: pt.DataFrame[BullBearSide]):
    """
    Find the time_frame_bbs_boundary which matches with each pivot:
        find all the time_frame_bbs_boundaries which the pivot is between its 'movement_start' and its 'movement_end'
        the boundary with the longest 'movement' is the best match
    :param pivots:
    :param bbs_boundaries:
    :return:
    """
    merge_bbs_overlap(time_frame_bbs=time_frame_bbs_trends)  # todo: test
    timeframe_pivots['bbs_movement_start'] = pd.Series(dtype='datetime64[ns, UTC]')
    timeframe_pivots['bbs_movement_start'] = pd.merge_asof(
        left=timeframe_pivots, right=time_frame_bbs_trends, left_index=True, right_on='movement_start_time',
        direction='forward'
    )[['movement_start_time']]


def ftc_of_range(timeframe_pivots, multi_timeframe_base_patterns, pivots_timeframe):
    pivot_lower_timeframes = config.timeframes[config.timeframes.index(pivots_timeframe):0:-1]  # todo: test
    remained_pivots = timeframe_pivots
    for timeframe in pivot_lower_timeframes:
        if len(remained_pivots) > 0:
            timeframe_base_patterns = single_timeframe(multi_timeframe_base_patterns, timeframe)
            remained_pivots['ftc_list'] = remained_pivots[
                (timeframe_base_patterns.index < remained_pivots.index)
                & (timeframe_base_patterns.index > remained_pivots['ftc_range_start_time'])
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
    if timeframe_shortlist is None:  # todo: test
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
    timeframe_pivots['bbs_movement_start'] = \
        pivot_bbs_movement_start(timeframe_pivots, time_frame_bbs_boundaries)['bbs_movement_start']  # todo: test
    pivots_with_bbs_movement = timeframe_pivots[timeframe_pivots['bbs_movement_start'].notna()]
    pivot_movement = \
        pivots_with_bbs_movement['bbs_movement_start'] - pivots_with_bbs_movement.index.get_level_values('date')
    pivots_with_bbs_movement['ftc_range_start_value'] = \
        pivots_with_bbs_movement.index.get_level_values('date') - pivot_movement * config.ftc_price_range_percentage
    ftc_range_start_time(ohlcv, pivots_with_bbs_movement)
    ftc_of_range(pivots_with_bbs_movement, multi_timeframe_base_patterns, timeframe)


def ftc_range_start_time(ohlcv, timeframe_pivots):
    resistance_pivots = timeframe_pivots[timeframe_pivots['is_resistance']]  # todo: test
    timeframe_pivots.loc[resistance_pivots.index, 'ftc_range_start_time'] = (
        insert_crossing2(base=resistance_pivots, base_target_column='ftc_range_start_value',
                         target=ohlcv, target_compare_column='low', direction='left',
                         more_significant=lambda target, base: target < base,
                         ))['left_crossing_time']
    support_pivots = timeframe_pivots[~timeframe_pivots['is_resistance']]
    timeframe_pivots.loc[support_pivots.index, 'ftc_range_start_time'] = (
        insert_crossing2(base=support_pivots, base_target_column='ftc_range_start_value',
                         target=ohlcv, target_compare_column='high', direction='left',
                         more_significant=lambda target, base: target > base,
                         ))['left_crossing_time']
