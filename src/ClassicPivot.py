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
from PanderaDFM.Pivot import MultiTimeframePivotDFM, PivotDFM, MultiTimeframePivotDf
from PeakValley import find_crossing
from PeakValleyPivots import read_multi_timeframe_major_times_top_pivots
from helper.data_preparation import single_timeframe, empty_df, concat, nearest_match
from helper.helper import measure_time, date_range
from ohlcv import read_base_timeframe_ohlcv


class LevelDirection(Enum):
    Support = 'support'
    Resistance = 'resistance'


def first_exited_bar(pivot: pt.DataFrame[PivotDFM], timeframe_ohlcv: pt.DataFrame[OHLCV]) \
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


def exit_bars(timeframe_active_pivots: pt.DataFrame[PivotDFM], timeframe_ohlcv: pt.DataFrame[OHLCV]) \
        -> list[Tuple[datetime, float]]:
    exit_bars = [
        first_exited_bar(start, pivot, timeframe_ohlcv)
        for start, pivot in timeframe_active_pivots.iterrows()
    ]
    return exit_bars


def passing_time(timeframe_active_pivots: pt.DataFrame[PivotDFM], timeframe_ohlcv: pt.DataFrame[OHLCV]):
    _exit_bars = exit_bars(timeframe_active_pivots, timeframe_ohlcv)
    return dict(_exit_bars).keys()


def insert_passing_time(may_have_passing: pt.DataFrame[PivotDFM], base_timeframe_ohlcv: pt.DataFrame[OHLCV]):
    support_pivots_index = may_have_passing[may_have_passing['is_support'] == True]  # todo: test
    may_have_passing.loc[support_pivots_index, 'passing_time'] = \
        find_crossing(may_have_passing.loc[support_pivots_index], 'external_margin',
                      base_timeframe_ohlcv, 'high', 'right', lambda base, target: target > base,
                      )

    resistance_pivots_index = may_have_passing[may_have_passing['is_support'] == False]
    may_have_passing.loc[resistance_pivots_index, 'passing_time'] = \
        find_crossing(may_have_passing.loc[resistance_pivots_index], 'external_margin',
                      base_timeframe_ohlcv, 'low', 'right', lambda base, target: target < base,
                      )

    valid_passing_pivots = may_have_passing[may_have_passing['passing'].notna()
                                            & (may_have_passing['passing'] < may_have_passing['ttl'])
                                            ]
    may_have_passing.loc[valid_passing_pivots, 'deactivated_at'] = may_have_passing.loc[valid_passing_pivots, 'passing']


def deactivate_by_ttl(pivots: pt.DataFrame[MultiTimeframePivotDFM], base_timeframe_ohlcv: pt.DataFrame[OHLCV]):
    end = base_timeframe_ohlcv.index.get_level_values(level='date').max()  # todo: test
    ttl_expired = pivots[
        (pivots['deactivated_at'].isna() | ~pivots['deactivated_at'])
        & (pivots['ttl'] < end)
        ].index
    pivots.loc[ttl_expired, 'deactivated_at'] = pivots.loc[ttl_expired, 'ttl']


def duplicate_on_passing_times(pivots: pt.DataFrame[MultiTimeframePivotDFM], base_timeframe_ohlcv: pt.DataFrame[OHLCV]):
    may_have_passing = pivots[pivots['deactivated_at'].isna()]  # todo: test
    while len(may_have_passing) > 0:  # len(old_pivots_with_passing) < pivots[pivots['passing'].notna()]:
        insert_passing_time(may_have_passing, base_timeframe_ohlcv)
        passed_pivots = may_have_passing[may_have_passing['passing'].notna()].index
        may_have_passing = MultiTimeframePivotDf.new()
        for (timeframe, pivot_date), pivot_info in passed_pivots.iterrows():
            passed_pivots.loc[(timeframe, pivot_date), 'deactivated_at'] = pivot_date['passing']
            new_pivot = MultiTimeframePivotDf.new({  # todo: debug from here
                'date': pivot_info['passing'],
                'level': pivot_info['level'],
                'is_resistance': not pivot_info['is_resistance'],
                'original_start': pivot_date['original_start'],
                'internal_margin': pivot_date['external_margin'],
                'external_margin': pivot_date['internal_margin'],
                'ttl': pivot_date['ttl'],
            })
            may_have_passing.loc[pivot_info['passing']] = new_pivot
        pivots = MultiTimeframePivotDf.concat(pivots, may_have_passing)


# def reactivate_passed_levels(timeframe_active_pivots: pt.DataFrame[PivotDFM],
#                              timeframe_inactive_pivots: pt.DataFrame[PivotDFM],
#                              timeframe_ohlcv: pt.DataFrame[OHLCV]
#                              ) -> Tuple[pt.DataFrame[PivotDFM], pt.DataFrame[PivotDFM]]:
#     # todo: test
#
#     # timeframe_active_pivots = update_pivot_side(timeframe_active_pivots, timeframe_ohlcv)
#     timeframe_active_pivots['passing_time'] = passing_time(timeframe_active_pivots, timeframe_ohlcv)
#     passed_pivots = timeframe_active_pivots[timeframe_active_pivots['passing_time'].notna()].index
#     timeframe_inactive_pivots: pt.DataFrame[PivotDFM] = (
#         concat(timeframe_inactive_pivots, timeframe_active_pivots.loc[passed_pivots]))
#     timeframe_active_pivots = timeframe_active_pivots.drop(labels=passed_pivots)
#     return timeframe_active_pivots, timeframe_inactive_pivots


def inactivate_3rd_hit(pivots: pt.DataFrame['MultiTimeframePivotDFM'], base_timeframe_ohlcv: pt.DataFrame[OHLCV]):
    # todo: test
    pivots = update_hit(pivots, base_timeframe_ohlcv)
    deactivate_by_hit = pivots[pivots['hit'] > config.pivot_number_of_active_hits].index
    pivots.loc[deactivate_by_hit, 'deactivated_at'] = \
        pivots.loc[deactivate_by_hit, f'hit_start_{config.pivot_number_of_active_hits - 1}']


def update_pivot_deactivation(multi_timeframe_pivots: pt.DataFrame['MultiTimeframePivotDFM'],
                              base_timeframe_ohlcv: pt.DataFrame[OHLCV]):
    """
        hit_count = number of pivots:
            after activation time
            between inner_margin and outer_margin of level
        if hit_count > 2: mark level as inactive.
    """
    # final_multi_timeframe_active_pivots = empty_df(MultiTimeframePivotDFM)
    # final_multi_timeframe_inactive_pivots = empty_df(MultiTimeframePivotDFM)
    # for timeframe in config.timeframes:
    #     timeframe_active_pivots = single_timeframe(multi_timeframe_active_pivots, timeframe).copy()
    #     timeframe_inactive_pivots = single_timeframe(multi_timeframe_inactive_pivots, timeframe).copy()
    duplicate_on_passing_times(multi_timeframe_pivots, base_timeframe_ohlcv)
    # reactivate_passed_levels(timeframe_active_pivots, timeframe_inactive_pivots))
    inactivate_3rd_hit(multi_timeframe_pivots)
    deactivate_by_ttl(multi_timeframe_pivots, base_timeframe_ohlcv)
    #     final_multi_timeframe_active_pivots = (concat(final_multi_timeframe_active_pivots, timeframe_active_pivots)
    #                                            .sort_index(level='date'))
    #     final_multi_timeframe_inactive_pivots = (
    #         concat(final_multi_timeframe_inactive_pivots, timeframe_inactive_pivots)
    #         .sort_index(level='date'))
    #
    # return multi_timeframe_active_pivots, multi_timeframe_inactive_pivots
    # archive_cold_levels(update__time, multi_timeframe_pivots)


def reactivated_passed_levels(_time, ohlcv: pt.DataFrame[OHLCV],
                              multi_timeframe_pivots: pt.DataFrame[MultiTimeframePivotDFM]) \
        -> pt.DataFrame[MultiTimeframePivotDFM]:
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


# def archive_cold_levels(_time, multi_timeframe_pivots: pt.DataFrame[MultiTimeframePivotDFM]) \
#         -> pt.DataFrame[MultiTimeframePivotDFM]:
#     for pivot_time, pivot_info in multi_timeframe_pivots.iterrows():
#         if _time > pivot_info['ttl']:
#             multi_timeframe_pivots.loc[pivot_time, 'archived_at'] = pivot_info['ttl']
#     return multi_timeframe_pivots


# def update_inactive_levels(update__time, ohlcv: pt.DataFrame[OHLCV],
#                            multi_timeframe_pivots: pt.DataFrame[MultiTimeframePivotDFM]) \
#         -> pt.DataFrame[MultiTimeframePivotDFM]:
#     """
#         archive levels which have been inactive for more than 16^2 intervals
#         if price moved from inner_margin to outer_margin: reset hit count to 0 and mark level as active
#     :return:
#     """
#     multi_timeframe_pivots = archive_cold_levels(update__time, multi_timeframe_pivots)
#     multi_timeframe_pivots = reactivated_passed_levels(multi_timeframe_pivots)
#
#     return multi_timeframe_pivots


# def update_pivot_side(pivot_levels: pt.DataFrame[Pivot], ohlcv: pt.DataFrame[OHLCV]) -> pt.DataFrame[Pivot]:
#     pivots_with_unknown_direction = pivot_levels[pivot_levels['direction'].isna()]
#     pivot_levels.loc[pivots_with_unknown_direction, 'first_exit_nearest_value'] = \
#         dict(exit_bars(pivot_levels.loc[pivots_with_unknown_direction], ohlcv)).values()
#     pivots_with_found_direction = pivot_levels[pivot_levels['first_exit_nearest_value'].notna()]
#     pivot_levels.loc[pivots_with_found_direction, 'direction'] = (
#         'support' if pivot_levels.loc[pivots_with_found_direction, 'first_exit_nearest_value'] >
#                      pivot_levels.loc[pivots_with_found_direction, ['internal_margin', 'external_margin']]
#                      .max(axis='column') else 'resistance'
#     )
#     return pivot_levels


def get_date_range_of_pivots_ftc():
    raise NotImplementedError
    _, end = date_range(config.processing_date_range)
    first_movement_start = active_pivot_levels['movement_start_time'].dropna().min()


# def update_levels(active_pivot_levels: pt.DataFrame[MultiTimeframePivotDFM],
#                   inactive_pivot_level: pt.DataFrame[MultiTimeframePivotDFM],
#                   archived_pivot_levels: pt.DataFrame[MultiTimeframePivotDFM],
#                   date_range_str: str = None):
#     active_pivot_levels, inactive_pivot_level = update_active_levels(active_pivot_levels, inactive_pivot_level)
#     active_pivot_levels, inactive_pivot_level = update_inactive_levels(active_pivot_levels, inactive_pivot_level)
#     insert_pivot_ftc(active_pivot_levels)


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
    if date_range_str is None:  # todo: test
        date_range_str = config.processing_date_range
    multi_timeframe_pivots = read_classic_pivots(date_range_str)
    for (pivot_timeframe, pivot_time), pivot_info in multi_timeframe_pivots.sort_index(level='date'):
        multi_timeframe_pivots.loc[(pivot_timeframe, pivot_time), 'is_overlap_of'] = \
            pivot_exact_overlapped(pivot_time, multi_timeframe_pivots)
    raise NotImplementedError
    # base_timeframe_ohlcv = read_base_timeframe_ohlcv()
    # update_pivot_deactivation(multi_timeframe_pivots, base_timeframe_ohlcv)


def insert_ftc(pivots: pt.DataFrame[MultiTimeframeAtrMovementPivotDFM], structure_timeframe_shortlist):
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


def read_classic_pivots(date_range_str) -> pt.DataFrame[MultiTimeframePivotDFM]:
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


def update_hit(active_timeframe_pivots: pt.DataFrame[PivotDFM], base_timeframe_ohlcv) -> pt.DataFrame[PivotDFM]:
    # todo: test
    if 'passed_time' not in active_timeframe_pivots.columns:
        AssertionError("Expected passing_time be calculated before: "
                       "! 'passed_time' not in active_timeframe_pivots.columns")
    t_pivot_indexes = active_timeframe_pivots.index
    active_timeframe_pivots.loc[t_pivot_indexes, 'boundary_of_hit_start_0'] = \
        nearest_match(needles=t_pivot_indexes, reference=base_timeframe_ohlcv, direction='forward', shift=1)
    for n in range(config.pivot_number_of_active_hits):
        if n == 0:
            active_timeframe_pivots.loc[t_pivot_indexes, 'boundary_of_hit_start_0'] = \
                nearest_match(needles=t_pivot_indexes, reference=base_timeframe_ohlcv, direction='forward', shift=1)
        else:
            active_timeframe_pivots.loc[t_pivot_indexes, f'boundary_of_hit_start_{n}'] = \
                nearest_match(needles=active_timeframe_pivots.loc[t_pivot_indexes, f'boundary_of_hit_end_{n - 1}'],
                              reference=base_timeframe_ohlcv, direction='forward', shift=1)
        active_timeframe_pivots.loc[t_pivot_indexes][f'hit_start_{n}'] = (
            insert_hits(active_timeframe_pivots.loc[t_pivot_indexes], f'boundary_of_hit_start_{n}', n, \
                        base_timeframe_ohlcv, 'start'))[f'hit_start_{n}']

        active_timeframe_pivots.loc[t_pivot_indexes, f'boundary_of_hit_end_{n}'] = \
            nearest_match(needles=active_timeframe_pivots.loc[t_pivot_indexes, f'boundary_of_hit_start_{n}'],
                          reference=base_timeframe_ohlcv, direction='forward', shift=1)
        active_timeframe_pivots.loc[t_pivot_indexes][f'hit_end_{n}'] = (
            insert_hits(active_timeframe_pivots.loc[t_pivot_indexes], f'boundary_of_hit_end_{n}', n, \
                        base_timeframe_ohlcv, 'end'))[f'hit_end_{n}']
        invalid_hit_ends = active_timeframe_pivots[active_timeframe_pivots.loc[t_pivot_indexes, f'hit_end_{n}'] <
                                                   active_timeframe_pivots.loc[t_pivot_indexes, f'passed']]
        active_timeframe_pivots.loc[invalid_hit_ends, f'hit_end_{n}'] = pd.NaT
        active_timeframe_pivots.loc[active_timeframe_pivots[f'hit_end_{n}'].notna(), 'hit'] = n
        t_pivot_indexes = active_timeframe_pivots[active_timeframe_pivots[f'hit_end_{n}'].notna()]

    return active_timeframe_pivots


def insert_hits(active_timeframe_pivots: pt.DataFrame[PivotDFM], hit_boundary_column: str, n: int,
                base_timeframe_ohlcv: pt.DataFrame[OHLCV],
                side: Literal['start', 'end']):
    # todo: test
    t_df: pd.DataFrame = active_timeframe_pivots.set_index(hit_boundary_column)
    resistance_indexes = t_df[t_df['is_resistance'] == True].index
    t_df.loc[resistance_indexes, f'hit_{side}_{n}'] = \
        insert_hit(t_df.loc[resistance_indexes], n, base_timeframe_ohlcv, side, 'Resistance') \
            [f'hit_{side}_{n}']
    t_support_indexes = t_df[t_df['is_resistance'] == False].index
    t_df.loc[t_support_indexes, f'hit_{side}_{n}'] = \
        insert_hit(t_df.loc[t_support_indexes], n, base_timeframe_ohlcv, side, 'Support') \
            [f'hit_{side}_{n}']
    t_df.set_index('date')
    return t_df


def insert_hit(pivots: pt.DataFrame[PivotDFM], n: int, base_timeframe_ohlcv: pt.DataFrame[OHLCV],
               side: Literal['start', 'end'], pivot_type: Literal['Resistance', 'Support']):
    if pivot_type == 'Resistance':  # todo: test
        def gt(base, target):
            return target >= base

        def lt(base, target):
            return target > base
    else:  # pivot_type == 'Support'
        def gt(base, target):
            return target <= base

        def lt(base, target):
            return target < base
    if side == 'start':
        pivots[f'hit_start_{n}'] = \
            find_crossing(pivots, 'internal_margin', base_timeframe_ohlcv, 'high',
                          'right', gt)
    else:
        pivots[f'hit_end_{n}'] = \
            find_crossing(pivots, 'internal_margin', base_timeframe_ohlcv, 'high',
                          'right', lt)

    if pivots['is_resistance'].isna().any():
        raise AssertionError("active_timeframe_pivots['is_resistance'].isna().any()")

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
