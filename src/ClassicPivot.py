from datetime import datetime
from enum import Enum
from typing import Tuple, Union, Literal

import pandas as pd
from pandera import typing as pt

from BasePattern import read_multi_timeframe_base_patterns
from BullBearSidePivot import read_multi_timeframe_bull_bear_side_pivots
from Config import config
from PanderaDFM.AtrTopPivot import MultiTimeframeAtrMovementPivotDFM
from PanderaDFM.OHLCV import OHLCV
from PanderaDFM.Pivot import MultiTimeframePivot, Pivot
from PeakValley import find_crossing
from PeakValleyPivots import read_multi_timeframe_major_times_top_pivots
from helper.data_preparation import single_timeframe, empty_df, concat
from helper.helper import measure_time, date_range


class LevelDirection(Enum):
    Support = 'support'
    Resistance = 'resistance'


def first_exited_bar(pivot: pt.DataFrame[Pivot], timeframe_ohlcv: pt.DataFrame[OHLCV]) \
        -> Tuple[Union[datetime, None], Union[float, None]]:
    if all(pivot['direction'] is None) != any(pivot['direction'] is None):
        raise Exception("'direction' should not be None for all rows or to be None for all rows.")
    t_ohlcv = timeframe_ohlcv.loc[pivot['activation_time']:]
    t_ohlcv['is_above'] = False
    t_ohlcv['is_below'] = False
    if pivot['direction'] != LevelDirection.Support:  # == LevelDirection.Resistance.value or level_direction is None:
        t_ohlcv['is_above'] = (
                t_ohlcv['low'] > pivot[['internal_margin', 'external_margin']].min(axis='columns')
        )
    if pivot['direction'] != LevelDirection.Resistance:  # == LevelDirection.Support.value or level_direction is None:
        t_ohlcv['is_below'] = (
                t_ohlcv['high'] < pivot[['internal_margin', 'external_margin']].max(axis='columns')
        )
    exited_bars = t_ohlcv[(t_ohlcv['is_above'] or t_ohlcv['is_below']) == True]
    if len(exited_bars) > 0:
        _first_exited_bar_time = exited_bars.index[0]
        value_column = 'high' if t_ohlcv.loc[_first_exited_bar_time, 'is_below'] else 'low'
        _first_exited_bar_value = t_ohlcv[value_column]
        return _first_exited_bar_time, _first_exited_bar_value
    else:
        return None, None


def exit_bars(timeframe_active_pivots: pt.DataFrame[Pivot], timeframe_ohlcv: pt.DataFrame[OHLCV]) \
        -> list[Tuple[datetime, float]]:
    exit_bars = [
        first_exited_bar(start, pivot, timeframe_ohlcv)
        for start, pivot in timeframe_active_pivots.iterrows()
    ]
    return exit_bars


def passing_time(timeframe_active_pivots: pt.DataFrame[Pivot], timeframe_ohlcv: pt.DataFrame[OHLCV]):
    _exit_bars = exit_bars(timeframe_active_pivots, timeframe_ohlcv)
    return dict(_exit_bars).keys()


def reactivate_passed_levels(timeframe_active_pivots: pt.DataFrame[Pivot],
                             timeframe_inactive_pivots: pt.DataFrame[Pivot],
                             timeframe_ohlcv: pt.DataFrame[OHLCV]
                             ) -> Tuple[pt.DataFrame[Pivot], pt.DataFrame[Pivot]]:
    # todo: test
    timeframe_active_pivots = update_pivot_side(timeframe_active_pivots, timeframe_ohlcv)
    timeframe_active_pivots['passing_time'] = passing_time(timeframe_active_pivots, timeframe_ohlcv)
    passed_pivots = timeframe_active_pivots[timeframe_active_pivots['passing_time'].notna()].index
    timeframe_inactive_pivots: pt.DataFrame[Pivot] = (
        concat(timeframe_inactive_pivots, timeframe_active_pivots.loc[passed_pivots]))
    timeframe_active_pivots = timeframe_active_pivots.drop(labels=passed_pivots)
    return timeframe_active_pivots, timeframe_inactive_pivots


def inactivate_3rd_hit(timeframe_active_pivots, timeframe_inactive_pivots):
    # todo: test
    timeframe_active_pivots = update_hit(timeframe_active_pivots)
    inactive_hits = timeframe_active_pivots[timeframe_active_pivots['hit'] > config.pivot_number_of_active_hits].index
    if len(inactive_hits.intersection(timeframe_inactive_pivots.index)) > 0:
        AssertionError("len(inactive_hits.intersection(timeframe_inactive_pivots.index))>0")
    timeframe_inactive_pivots.loc[inactive_hits] = timeframe_active_pivots.loc[inactive_hits]
    timeframe_active_pivots.drop(index=inactive_hits, inplace=True)


def update_active_levels(multi_timeframe_active_pivots: pt.DataFrame[MultiTimeframePivot],
                         multi_timeframe_inactive_pivots: pt.DataFrame[MultiTimeframePivot],
                         ) -> Tuple[pt.DataFrame[MultiTimeframePivot], pt.DataFrame[MultiTimeframePivot]]:
    """
        hit_count = number of pivots:
            after activation time
            between inner_margin and outer_margin of level
        if hit_count > 2: mark level as inactive.
    """
    final_multi_timeframe_active_pivots = empty_df(MultiTimeframePivot)
    final_multi_timeframe_inactive_pivots = empty_df(MultiTimeframePivot)
    for timeframe in config.timeframes:
        timeframe_active_pivots = single_timeframe(multi_timeframe_active_pivots, timeframe).copy()
        timeframe_inactive_pivots = single_timeframe(multi_timeframe_inactive_pivots, timeframe).copy()
        timeframe_active_pivots, timeframe_inactive_pivots = (
            reactivate_passed_levels(timeframe_active_pivots, timeframe_inactive_pivots))
        timeframe_active_pivots, timeframe_inactive_pivots = (
            inactivate_3rd_hit(timeframe_active_pivots, timeframe_inactive_pivots))

        final_multi_timeframe_active_pivots = (concat(final_multi_timeframe_active_pivots, timeframe_active_pivots)
                                               .sort_index(level='date'))
        final_multi_timeframe_inactive_pivots = (
            concat(final_multi_timeframe_inactive_pivots, timeframe_inactive_pivots)
            .sort_index(level='date'))

    return multi_timeframe_active_pivots, multi_timeframe_inactive_pivots


def reactivated_passed_levels(_time, ohlcv: pt.DataFrame[OHLCV],
                              multi_timeframe_pivots: pt.DataFrame[MultiTimeframePivot]) \
        -> pt.DataFrame[MultiTimeframePivot]:
    """
        if price moved from inner_margin to outer_margin: reset hit count to 0 and mark level as active
        for any of inactive levels:
            find boundaries starts after level has been inactivated
            merge adjacent boundaries and use highest high and lowest low for merged boundaries.
            if any boundary which level is between boundary's low and high:
                reactivate level
    :return:
    """
    inactive_pivots = multi_timeframe_pivots[
        multi_timeframe_pivots['archived_at'].isnull()
        & (not multi_timeframe_pivots['deactivated_at'].isnull())
        ].sort_values(by='deactivated_at')
    for inactive_pivot_index, _ in inactive_pivots.iterrows():
        filtered_ohlcv = ohlcv[inactive_pivot_index:_time]

    raise Exception('Not implemented')


def archive_cold_levels(_time, multi_timeframe_pivots: pt.DataFrame[MultiTimeframePivot]) \
        -> pt.DataFrame[MultiTimeframePivot]:
    for pivot_time, pivot_info in multi_timeframe_pivots.iterrows():
        if _time > pivot_info['ttl']:
            multi_timeframe_pivots.loc[pivot_time, 'archived_at'] = pivot_info['ttl']
    return multi_timeframe_pivots


def update_inactive_levels(update__time, ohlcv: pt.DataFrame[OHLCV],
                           multi_timeframe_pivots: pt.DataFrame[MultiTimeframePivot]) \
        -> pt.DataFrame[MultiTimeframePivot]:
    """
        archive levels which have been inactive for more than 16^2 intervals
        if price moved from inner_margin to outer_margin: reset hit count to 0 and mark level as active
    :return:
    """
    multi_timeframe_pivots = archive_cold_levels(update__time, multi_timeframe_pivots)
    multi_timeframe_pivots = reactivated_passed_levels(multi_timeframe_pivots)

    return multi_timeframe_pivots


def update_pivot_side(pivot_levels: pt.DataFrame[Pivot], ohlcv: pt.DataFrame[OHLCV]) -> pt.DataFrame[Pivot]:
    pivots_with_unknown_direction = pivot_levels[pivot_levels['direction'].isna()]
    pivot_levels.loc[pivots_with_unknown_direction, 'first_exit_nearest_value'] = \
        dict(exit_bars(pivot_levels.loc[pivots_with_unknown_direction], ohlcv)).values()
    pivots_with_found_direction = pivot_levels[pivot_levels['first_exit_nearest_value'].notna()]
    pivot_levels.loc[pivots_with_found_direction, 'direction'] = (
        'support' if pivot_levels.loc[pivots_with_found_direction, 'first_exit_nearest_value'] >
                     pivot_levels.loc[pivots_with_found_direction, ['internal_margin', 'external_margin']]
                     .max(axis='column') else 'resistance'
    )
    return pivot_levels


def get_date_range_of_pivots_ftc():
    raise NotImplementedError
    _, end = date_range(config.processing_date_range)
    first_movement_start = active_pivot_levels['movement_start_time'].dropna().min()


def update_levels(active_pivot_levels: pt.DataFrame[MultiTimeframePivot],
                  inactive_pivot_level: pt.DataFrame[MultiTimeframePivot],
                  archived_pivot_levels: pt.DataFrame[MultiTimeframePivot],
                  date_range_str: str = None):
    active_pivot_levels, inactive_pivot_level = update_active_levels(active_pivot_levels, inactive_pivot_level)
    active_pivot_levels, inactive_pivot_level = update_inactive_levels(active_pivot_levels, inactive_pivot_level)
    insert_pivot_ftc(active_pivot_levels)


@measure_time
def generate_classic_pivots(date_range_str: str = None):
    """
    definition of pivot:
        highest high of every Bullish and lowest low of every Bearish trend. for Trends
            conditions:
                >= 3 atr
                >= 1 atr reverse movement after the most significant top
            index = highest high for Bullish and lowest low for Bearish
            highest high is not the last peak of Bullish and lowest low is not the last Valley raise a warning log:
                timeframe, trend start time (index), time of last top
                time and high of highest high in Bullish and time and low of lowest low in Bearish,
        same color trends >= 3 atr + reverse same color trend >= 1 atr
            condition:

    pivot information:
        index: datetime
    """
    raise NotImplementedError
    if date_range_str is None:
        date_range_str = config.processing_date_range
    multi_timeframe_pivots = read_classic_pivots(date_range_str)
    for (pivot_timeframe, pivot_time), pivot_info in multi_timeframe_pivots.sort_index(level='date'):
        multi_timeframe_pivots.loc[(pivot_timeframe, pivot_time), 'is_overlap_of'] = \
            pivot_exact_overlapped(pivot_time, multi_timeframe_pivots)
    update_levels()


def insert_pivot_ftc(pivots: pt.DataFrame[MultiTimeframeAtrMovementPivotDFM], structure_timeframe_shortlist):
    """
    The FTC is a base pattern in the last 38% of pivot movement price range.
    The FTC base pattern should start within the movement time range.
    The base pattern timeframe should be below the pivot timeframe.

    :param base_patterns:
    :param pivots:
    :param structure_timeframe_shortlist:
    :return:
    """
    raise NotImplementedError
    date_range_of_pivots_ftc = get_date_range_of_pivots_ftc()
    base_patterns = read_multi_timeframe_base_patterns(date_range_of_pivots_ftc)

    for pivot_index, pivot_info in pivots:
        ftcs =


def read_classic_pivots(date_range_str) -> pt.DataFrame[MultiTimeframePivot]:
    multi_timeframe_bull_bear_side_pivots = read_multi_timeframe_bull_bear_side_pivots(date_range_str)
    multi_timeframe_anti_pattern_tops_pivots = read_multi_timeframe_major_times_top_pivots(date_range_str)
    # multi_timeframe_color_trend_pivots = read_multi_timeframe_color_trend_pivots()
    multi_timeframe_pivots = concat(
        multi_timeframe_bull_bear_side_pivots,
        # multi_timeframe_color_trend_pivots,
        multi_timeframe_anti_pattern_tops_pivots,
    )
    return multi_timeframe_pivots


def pivot_exact_overlapped(pivot_time, multi_timeframe_pivots):
    # todo: try to merge with update_hit
    # new_pivots['is_overlap_of'] = None
    if not len(multi_timeframe_pivots) > 0:
        return None

    # for pivot_time, pivot in new_pivots.iterrows():
    overlapping_major_timeframes = \
        multi_timeframe_pivots[multi_timeframe_pivots.index.get_level_values('date') == pivot_time]
    if len(overlapping_major_timeframes) > 0:
        root_pivot = overlapping_major_timeframes[
            multi_timeframe_pivots['is_overlap_of'].isna()
        ]
        if len(root_pivot) == 1:
            # new_pivots.loc[pivot_time, 'is_overlap_of'] = \
            return root_pivot.index.get_level_values('timeframe')[0]
        else:
            raise Exception(f'Expected to find only one root_pivot but found({len(root_pivot)}):{root_pivot}')
    # return new_pivots
    # raise Exception(f'Expected to find a root_pivot but found zero')


def update_hit(active_timeframe_pivots: pt.DataFrame[Pivot]) -> pt.DataFrame[Pivot]:
    # todo: test
    if 'passed_time' not in active_timeframe_pivots.columns:
        AssertionError("Expected passing_time be calculated before: "
                       "! 'passed_time' not in active_timeframe_pivots.columns")
    t_pivot_indexes = active_timeframe_pivots.index
    for n in range(config.pivot_number_of_active_hits):
        active_timeframe_pivots.loc[t_pivot_indexes][f'hit_start_{n}'] = (
            insert_hit_start(active_timeframe_pivots.loc[t_pivot_indexes], n))[f'hit_start_{n}']
        active_timeframe_pivots.loc[t_pivot_indexes][f'hit_end_{n}'] = (
            insert_hit_end(active_timeframe_pivots.loc[t_pivot_indexes], n))[f'hit_end_{n}']
        invalid_hit_ends = active_timeframe_pivots[active_timeframe_pivots.loc[t_pivot_indexes, f'hit_end_{n}'] <
                                                   active_timeframe_pivots.loc[t_pivot_indexes, f'passed']]
        active_timeframe_pivots.loc[invalid_hit_ends, f'hit_end_{n}'] = pd.NaT
        active_timeframe_pivots.loc[active_timeframe_pivots[f'hit_end_{n}'].notna(), 'hit'] = n
        t_pivot_indexes = active_timeframe_pivots[active_timeframe_pivots[f'hit_end_{n}'].notna()]

    # for start, pivot in active_timeframe_pivots.iterrows():
    #     may_hit_timeframe_pivots = \
    #         all_timeframe_pivots.loc[pivot['activation_time']:].copy()
    #     may_hit_timeframe_pivots = \
    #         may_hit_timeframe_pivots.loc[:(pivot[['deactivated_at', 'ttl']].min(axis='columns'))]
    #     hitting_timeframe_pivots = may_hit_timeframe_pivots[
    #         (may_hit_timeframe_pivots['level'] <
    #          may_hit_timeframe_pivots[['internal_margin', 'external_margin']].max(axis='columns')) &
    #         (may_hit_timeframe_pivots['level'] >
    #          may_hit_timeframe_pivots[['internal_margin', 'external_margin']].min(axis='columns'))
    #         ]
    #     if len(hitting_timeframe_pivots) > 0:
    #         active_timeframe_pivots.loc[start, 'hit_count'] = len(hitting_timeframe_pivots)
    return active_timeframe_pivots

def insert_hits(active_timeframe_pivots: pt.DataFrame[Pivot], n: int, base_timeframe_ohlcv: pt.DataFrame[OHLCV],
                     side: Literal['start', 'end']):
    # todo: test
    resistance_indexes = active_timeframe_pivots[active_timeframe_pivots['is_resistance'] == True].index
    active_timeframe_pivots.loc[resistance_indexes, f'hit_{side}_{n}'] = \
        insert_hit(active_timeframe_pivots.loc[resistance_indexes], n, base_timeframe_ohlcv, side, 'Resistance')\
            [f'hit_{side}_{n}']
    t_support_indexes = active_timeframe_pivots[active_timeframe_pivots['is_resistance'] == False].index
    active_timeframe_pivots.loc[t_support_indexes, f'hit_{side}_{n}'] = \
        insert_hit(active_timeframe_pivots.loc[t_support_indexes], n, base_timeframe_ohlcv, side, 'Resistance')\
            [f'hit_{side}_{n}']

def insert_hit(active_timeframe_pivots: pt.DataFrame[Pivot], n: int, base_timeframe_ohlcv: pt.DataFrame[OHLCV],
                     side: Literal['start', 'end'], pivot_type: Literal['Resistance', 'Support']):
    # todo: test
    if active_timeframe_pivots['is_resistance'].isna().any():
        raise AssertionError("active_timeframe_pivots['is_resistance'].isna().any()")

    active_timeframe_pivots.loc[t_resistance_indexes, f'hit_start_{n}'] = \
        find_crossing(active_timeframe_pivots.loc[t_resistance_indexes], 'internal_margin',
                      base_timeframe_ohlcv, 'high', 'right', 'left', lambda base, target: target > base)

    active_timeframe_pivots.loc[t_support_indexes, f'hit_start_{n}'] = \
        find_crossing(active_timeframe_pivots.loc[t_support_indexes], 'internal_margin',
                      base_timeframe_ohlcv, 'high', 'right', 'left', lambda base, target: target > base)
def insert_hit_end(active_timeframe_pivots: pt.DataFrame[Pivot], n: int, base_timeframe_ohlcv: pt.DataFrame[OHLCV]):
    # todo: test
    if active_timeframe_pivots['is_resistance'].isna().any():
        raise AssertionError("active_timeframe_pivots['is_resistance'].isna().any()")
    t_resistance_indexes = active_timeframe_pivots[active_timeframe_pivots['is_resistance'] == True].index
    active_timeframe_pivots.loc[t_resistance_indexes, f'hit_start_{n}'] = \
        find_crossing(active_timeframe_pivots.loc[t_resistance_indexes], 'internal_margin',
                      base_timeframe_ohlcv, 'high', 'right', 'left', lambda base, target: target > base)
    t_support_indexes = active_timeframe_pivots[active_timeframe_pivots['is_resistance'] == False].index
    active_timeframe_pivots.loc[t_support_indexes, f'hit_start_{n}'] = \
        find_crossing(active_timeframe_pivots.loc[t_support_indexes], 'internal_margin',
                      base_timeframe_ohlcv, 'high', 'right', 'left', lambda base, target: target > base)

# def old_update_hit(active_multi_timeframe_pivots: pt.DataFrame[MultiTimeframePivot]) \
#         -> pt.DataFrame[MultiTimeframePivot]:
#     """
#     number of pivots:
#         after activation time
#         between inner_margin and outer_margin of level
#     if hit_count > 2: mark level as inactive.
#     if price moved from inner_margin to outer_margin: reset hit count to 0 and mark level as active
#
#     :return:
#     """
#     # multi_timeframe_pivots = update_inactive_levels(multi_timeframe_pivots)
#     # active_multi_timeframe_pivots = multi_timeframe_pivots[multi_timeframe_pivots['deactivated_at'].isnull()]
#     pivots_low_margin = active_multi_timeframe_pivots['internal_margin', 'external_margin'].min()
#     pivots_high_margin = active_multi_timeframe_pivots['internal_margin', 'external_margin'].max()
#     overlapping_pivots = active_multi_timeframe_pivots[
#         (level_info['level'] >= pivots_low_margin)
#         & (level_info['level'] <= pivots_high_margin)
#         ]
#     root_overlapping_pivots = overlapping_pivots[overlapping_pivots['is_overlap_of'].isnull()].sort_index(level='date')
#     root_overlapping_pivot = root_overlapping_pivots[-1]
#     active_multi_timeframe_pivots.loc[level_index, 'is_overlap_of'] = root_overlapping_pivot.index
#     active_multi_timeframe_pivots.loc[root_overlapping_pivot.index, 'hit'] += 1
#     if root_overlapping_pivot['hit'] > config.LEVEL_MAX_HIT:
#         active_multi_timeframe_pivots.loc[root_overlapping_pivot.index, 'deactivated_at'] = level_index
#
#     return multi_timeframe_pivots
