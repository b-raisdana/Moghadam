from typing import Tuple

from numpy import datetime64
from pandera import typing as pt

from Config import config
from PanderaDFM.BasePattern import MultiTimeframeBasePattern
from PanderaDFM.BullBearSide import BullBearSide
from PanderaDFM.OHLCV import OHLCV
from PanderaDFM.Pivot import PivotDFM
from PeakValley import insert_crossing2
from helper.data_preparation import single_timeframe


def bbs_movement_start(timeframe_pivots: pt.DataFrame[PivotDFM],
                       time_frame_bbs_boundaries: pt.DataFrame[BullBearSide]):
    """
    Find the time_frame_bbs_boundary which matchs with each pivot:
        find all the time_frame_bbs_boundaries which the pivot is between its 'movement_start' and its 'movement_end'
        the boundary with the longest 'movement' is the best match
    :param pivots:
    :param bbs_boundaries:
    :return:
    """
    raise NotImplementedError


def ftcs(timeframe_pivots, multi_timeframe_base_patterns, pivots_timeframe):
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


def find_ftc(timeframe_pivots: pt.DataFrame[PivotDFM], time_frame_bbs_boundaries: pt.DataFrame[BullBearSide],
             ohlcv: pt.DataFrame[OHLCV], multi_timeframe_base_patterns: pt.DataFrame[MultiTimeframeBasePattern],
             timeframe):
    timeframe_pivots['bbs_movement_start'] = \
        bbs_movement_start(timeframe_pivots, time_frame_bbs_boundaries)['bbs_movement_start']  # todo: test
    pivot_movement = timeframe_pivots['bbs_movement_start'] - timeframe_pivots.index.get_level_values('date')
    timeframe_pivots['ftc_range_start_value'] = \
        timeframe_pivots.index.get_level_values('date') - pivot_movement * config.ftc_price_range_percentage
    ftc_range_start_time(ohlcv, timeframe_pivots)
    ftcs(timeframe_pivots, multi_timeframe_base_patterns, timeframe)


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
