from typing import List

import pandas as pd
from pandera import typing as pt

import SupportResistance
from ClassicPivot import LevelDirection
from Config import config, TREND, TopTYPE
from PanderaDFM.BasePattern import MultiTimeframeBasePattern
from PanderaDFM.BullBearSide import BullBearSide, MultiTimeframeBullBearSide
from PanderaDFM.OHLCV import OHLCV, MultiTimeframeOHLCV
from PanderaDFM.PeakValley import PeakValley, MultiTimeframePeakValley
from PanderaDFM.Pivot import PivotDFM
from PanderaDFM.Pivot2 import MultiTimeframePivot2DFM
from PeakValley import insert_crossing2, peaks_only, valleys_only
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
        return time_frame_bbs
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
    if len(valid_same_start_overlaps['next_movement_index'].dropna().unique()) > 0:
        pass
    time_frame_bbs.drop(labels=valid_same_start_overlaps['next_movement_index'].dropna().unique(),
                        inplace=True)
    next_movements.drop(labels=valid_same_start_overlaps['next_movement_index'].dropna().unique(),
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
    no_merge_overlap_movements = overlapped_movements[
        overlapped_movements['next_movement_end_time'] <= overlapped_movements['movement_end_time']
        ]
    if len(no_merge_overlap_movements) > 0:
        time_frame_bbs.drop(labels=no_merge_overlap_movements['next_movement_index'].dropna().unique(),
                            inplace=True)
    # expand the end of first overlapping movement, to the end of second one to cover both

    # to_merge_overlap_movements = time_frame_bbs[
    #     time_frame_bbs['next_movement_index'].notna()
    #     # & (time_frame_bbs['next_movement_end_time'] > time_frame_bbs['movement_end_time'])
    # ]
    to_merge_overlap_movements = \
        overlapped_movements.loc[overlapped_movements.index.difference(no_merge_overlap_movements.index)]
    if len(to_merge_overlap_movements) > 0:
        time_frame_bbs.loc[to_merge_overlap_movements['date_backup'], 'movement_end_time'] = \
            to_merge_overlap_movements['next_movement_end_time'].to_list()
        # drop trends which covered by the original after expansion.
        time_frame_bbs.drop(
            labels=to_merge_overlap_movements['next_movement_index'].dropna().unique(), inplace=True)
    return time_frame_bbs


def zz_pivot_bull_bear_movement(timeframe_pivots: pt.DataFrame[PivotDFM],
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
    bull_bear_trends = (
        time_frame_bbs_trends[time_frame_bbs_trends['bull_bear_side'].isin([TREND.BULLISH.value, TREND.BEARISH.value])]
        .copy())
    no_overlap_bull_bear_trends = merge_bbs_overlap(time_frame_bbs=bull_bear_trends)
    no_overlap_bull_bear_trends = \
        no_overlap_bull_bear_trends.rename(columns={
            'movement_start_time': 'bbs_movement_start_time',
            'movement_start_value': 'bbs_movement_start_value',
        })[['bbs_movement_start_time', 'bbs_movement_start_value', ]]
    timeframe_pivots.loc[:, ['bbs_movement_start_time', 'bbs_movement_start_value']] = pd.merge_asof(
        left=timeframe_pivots, right=no_overlap_bull_bear_trends.dropna(), left_index=True,
        right_on='bbs_movement_start_time',
        direction='forward'
    )[['bbs_movement_start_time', 'bbs_movement_start_value']]
    timeframe_pivots['bbs_movement'] = \
        timeframe_pivots['bbs_movement_start_value'] - timeframe_pivots['level']  # todo: test


def ftc_of_range_by_time(pivots_with_ftc_range_start_time, multi_timeframe_base_patterns, pivots_timeframe):
    if len(pivots_with_ftc_range_start_time) > 0:
        pass
    pivot_lower_timeframes = config.timeframes[config.timeframes.index(pivots_timeframe):0:-1]
    pivots_with_ftc_range_start_time['ftc_list'] = pd.NA
    remained_pivots = pivots_with_ftc_range_start_time.copy()
    for timeframe in pivot_lower_timeframes:
        if len(remained_pivots) > 0:
            timeframe_base_patterns = single_timeframe(multi_timeframe_base_patterns, timeframe)
            for pivot_start, pivot in remained_pivots.iterrows():
                remained_pivots.loc[pivot_start, 'ftc_list'] = timeframe_base_patterns[
                    (timeframe_base_patterns.index < pivot_start)
                    & (timeframe_base_patterns.index > pivot['ftc_range_start_time'])
                    ]
                pivots_with_ftc_range_start_time.loc[pivot_start, 'ftc_list'] = \
                    remained_pivots.loc[pivot_start, 'ftc_list']
            remained_pivots = remained_pivots[remained_pivots['ftc_list'].isna()]
        else:
            break


def ftc_of_range_by_price(pivots_with_bbs_movement: pt.DataFrame[MultiTimeframePivot2DFM],
                          multi_timeframe_base_patterns: pt.DataFrame[MultiTimeframeBasePattern], pivots_timeframe):
    if len(pivots_with_bbs_movement) > 0:
        pass
    pivot_lower_timeframes = config.timeframes[config.timeframes.index(pivots_timeframe):0:-1]
    # todo: test
    pivots_with_bbs_movement['ftc_range_start_value'] = \
        pivots_with_bbs_movement['level'] - pivots_with_bbs_movement['bbs_movement'] * config.ftc_price_range_percentage
    pivots_with_bbs_movement['ftc_range_low'] = pivots_with_bbs_movement[['ftc_range_start_value', 'level']].min(
        axis='columns')
    pivots_with_bbs_movement['ftc_range_high'] = pivots_with_bbs_movement[['ftc_range_start_value', 'level']].max(
        axis='columns')

    pivots_with_bbs_movement['ftc_list'] = pd.NA
    remained_pivots = pivots_with_bbs_movement.copy()
    for timeframe in pivot_lower_timeframes:
        if len(remained_pivots) > 0:
            timeframe_base_patterns = single_timeframe(multi_timeframe_base_patterns, timeframe)
            for pivot_start, pivot in remained_pivots.iterrows():
                remained_pivots.loc[pivot_start, 'ftc_list'] = timeframe_base_patterns[
                    (timeframe_base_patterns['internal_low'] < pivot['ftc_range_high'])
                    & (timeframe_base_patterns['internal_high'] > pivot['ftc_range_low'])
                    ]
                pivots_with_bbs_movement.loc[pivot_start, 'ftc_list'] = \
                    remained_pivots.loc[pivot_start, 'ftc_list']
            remained_pivots = remained_pivots[remained_pivots['ftc_list'].isna()]
        else:
            break


def multi_timeframe_ftc(
        mt_pivot: pt.DataFrame[MultiTimeframePivot2DFM],
        mt_bbs_trend: pt.DataFrame[MultiTimeframeBullBearSide],
        mt_peaks_n_valleys: pt.DataFrame[MultiTimeframePeakValley],
        mt_ohlcv: pt.DataFrame[MultiTimeframeOHLCV],
        multi_timeframe_base_patterns: pt.DataFrame[MultiTimeframeBasePattern],
        timeframe_shortlist: List[str] = None):
    if timeframe_shortlist is None:
        timeframe_shortlist = config.structure_timeframes[::-1]
    else:  # filter and order
        timeframe_shortlist = [timeframe for timeframe in config.structure_timeframes[::-1]
                               if timeframe in timeframe_shortlist]
    mt_pivot.loc[:, 'ftc_list'] = pd.NA
    timeframe_shortlist = [timeframe for timeframe in timeframe_shortlist
                           if timeframe in mt_pivot.index.get_level_values(level='timeframe')]
    for timeframe in timeframe_shortlist:
        timeframe_pivots = single_timeframe(mt_pivot, timeframe)
        timeframe_bbs = single_timeframe(mt_bbs_trend, timeframe)
        time_frame_peaks_n_valleys = single_timeframe(mt_peaks_n_valleys)
        ohlcv = single_timeframe(mt_ohlcv, timeframe)
        pivots_with_ftc = insert_ftc(
            timeframe_pivots=timeframe_pivots,
            time_frame_bbs_boundaries=timeframe_bbs,
            time_frame_peaks_n_valleys=time_frame_peaks_n_valleys,
            ohlcv=ohlcv, multi_timeframe_base_patterns=multi_timeframe_base_patterns,
            timeframe=timeframe)
        pivots_with_ftc['timeframe'] = timeframe
        pivots_with_ftc = pivots_with_ftc.reset_index().set_index(['timeframe', 'date', 'original_start'])
        mt_pivot.loc[pivots_with_ftc.index, 'ftc_list'] = pivots_with_ftc['ftc_list']
        pass

def insert_pivot_movement(support_resistance: pt.DataFrame[PivotDFM], peaks_or_valleys: pt.DataFrame[PeakValley],
                          top_type: TopTYPE):
    '''
    real_start of support/resistance pivot is the first valley/peak which in compare with previous valley/peak is
    less_significant: (valley < previous / peak > previous)
    :param original_pivots:
    :param peaks_n_valleys:
    :return:
    '''
    if top_type == TopTYPE.PEAK: #Resistance:  # todo: test
        more_significant = lambda top, previous_top: top > previous_top
        high_low = 'high'
    else: # top_type == TopTYPE.VALLEY: #Support
        more_significant = lambda top, previous_top: top < previous_top
        high_low = 'low'
    for pivot_time, pivot in support_resistance.iterrows():
        passed_tops = peaks_or_valleys[peaks_or_valleys.index.get_level_values('date') < pivot_time]
        passed_tops['previous_top_value'] = passed_tops[high_low].shift(1) # todo: test
        movement_start = passed_tops[more_significant(passed_tops[high_low], passed_tops['previous_top_value'])]
        support_resistance.loc[pivot_time, 'real_start'] = movement_start.iloc[-1][high_low]


def insert_pivots_movement(original_pivots: pt.DataFrame[PivotDFM], peaks_n_valleys: pt.DataFrame[PeakValley]):
    resistance_pivots = original_pivots[original_pivots['is_resistance'].astype(bool)]
    peaks = peaks_only(peaks_n_valleys)
    resistance_pivots.loc[:, 'real_start'] = insert_pivot_movement(resistance_pivots, peaks)
    if 'real_start' not in original_pivots.columns:
        raise AssertionError("'real_start' not in original_pivots.columns") # todo: test
    support_pivots = original_pivots[~original_pivots['is_resistance'].astype(bool)]
    valleys = valleys_only(peaks_n_valleys)
    support_pivots.loc[:, 'real_start'] = insert_pivot_movement(support_pivots, valleys)


def insert_ftc(timeframe_pivots: pt.DataFrame[PivotDFM], time_frame_bbs_boundaries: pt.DataFrame[BullBearSide],
               time_frame_peaks_n_valleys: pt.DataFrame[PeakValley],
               ohlcv: pt.DataFrame[OHLCV], multi_timeframe_base_patterns: pt.DataFrame[MultiTimeframeBasePattern],
               timeframe):
    # todo: after testing properly, extend the operation to support duplicated pivots.
    original_pivots = timeframe_pivots[
        timeframe_pivots.index.get_level_values(level='date')
        == timeframe_pivots.index.get_level_values(level='original_start')] \
        .copy().reset_index().set_index('date')
    # pivot_bull_bear_movement(original_pivots, time_frame_bbs_boundaries)
    insert_pivots_movement(original_pivots, time_frame_peaks_n_valleys)
    if 'real_start' not in original_pivots.columns:
        raise AssertionError("'real_start' not in original_pivots.columns")  # todo: test
    # #
    # # if 'bbs_movement' not in original_pivots.columns:
    # #     raise AssertionError("'bbs_movement' not in original_pivots.columns")
    # pivots_with_bbs_movement = original_pivots[original_pivots['bbs_movement_start_value'].notna()].copy()
    # # pivots_with_bbs_movement['bbs_movement'] = \
    # #     pivots_with_bbs_movement['bbs_movement_start_value'] - pivots_with_bbs_movement['level']
    # # pivots_with_bbs_movement['ftc_range_start_value'] = \
    # #     pivots_with_bbs_movement['level'] - pivots_with_bbs_movement['bbs_movement'] * config.ftc_price_range_percentage
    # # ftc_range_start_time(pivots_with_bbs_movement, ohlcv)
    # # ftc_of_range_by_time(pivots_with_bbs_movement, multi_timeframe_base_patterns, timeframe)
    ftc_of_range_by_price(pivots_with_bbs_movement, multi_timeframe_base_patterns, timeframe)
    if 'ftc_range_start_value' not in pivots_with_bbs_movement.columns:
        raise AssertionError("'ftc_range_start_value' not in pivots_with_bbs_movement.columns")  # todo: test
    if 'ftc_range_low' not in pivots_with_bbs_movement.columns:
        raise AssertionError("'ftc_range_low' not in pivots_with_bbs_movement.columns")  # todo: test
    if 'ftc_range_high' not in pivots_with_bbs_movement.columns:
        raise AssertionError("'ftc_range_high' not in pivots_with_bbs_movement.columns")  # todo: test
    if len(pivots_with_bbs_movement) > 0:
        pass
    return pivots_with_bbs_movement


def ftc_range_start_time(pivots_with_bbs_movement, ohlcv):
    if len(pivots_with_bbs_movement) > 0:
        pass
    resistance_pivots = pivots_with_bbs_movement[pivots_with_bbs_movement['is_resistance']]
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
