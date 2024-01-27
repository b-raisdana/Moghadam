import warnings
from enum import Enum
from typing import Literal, List

import pandas as pd
from pandera import typing as pt

from BullBearSidePivot import read_multi_timeframe_bull_bear_side_pivots
from Config import config, TopTYPE
from PanderaDFM.AtrTopPivot import AtrMovementPivotDf
from PanderaDFM.OHLCV import OHLCV
from PanderaDFM.OHLCVA import OHLCVA
from PanderaDFM.Pivot import MultiTimeframePivotDFM, PivotDFM, PivotDf
from PeakValley import find_crossing
from PeakValleyPivots import read_multi_timeframe_major_times_top_pivots
from PivotsHelper import pivot_margins, level_ttl
from helper.data_preparation import concat, nearest_match
from helper.helper import measure_time


class LevelDirection(Enum):
    Support = 'support'
    Resistance = 'resistance'


# def first_exited_bar(pivot: pt.DataFrame[PivotDFM], timeframe_ohlcv: pt.DataFrame[OHLCV]) \
#         -> Tuple[Union[datetime, None], Union[float, None]]:
#     if all(pivot['direction'] is None) != any(pivot['direction'] is None):
#         raise Exception("'direction' should not be None for all rows or to be None for all rows.")
#     t_ohlcv = timeframe_ohlcv.loc[pivot['activation_time']:]
#     t_ohlcv['is_above'] = False
#     t_ohlcv['is_below'] = False
#     if pivot['direction'] != LevelDirection.Support:  # == LevelDirection.Resistance.value or level_direction is None:
#         t_ohlcv['is_above'] = (
#                 t_ohlcv['low'] > pivot[['internal_margin', 'external_margin']].min(axis='columns')
#         )
#     if pivot['direction'] != LevelDirection.Resistance:  # == LevelDirection.Support.value or level_direction is None:
#         t_ohlcv['is_below'] = (
#                 t_ohlcv['high'] < pivot[['internal_margin', 'external_margin']].max(axis='columns')
#         )
#     exited_bars = t_ohlcv[(t_ohlcv['is_above'] or t_ohlcv['is_below']) == True]
#     if len(exited_bars) > 0:
#         _first_exited_bar_time = exited_bars.index[0]
#         value_column = 'high' if t_ohlcv.loc[_first_exited_bar_time, 'is_below'] else 'low'
#         _first_exited_bar_value = t_ohlcv[value_column]
#         return _first_exited_bar_time, _first_exited_bar_value
#     else:
#         return None, None
#

# def exit_bars(timeframe_active_pivots: pt.DataFrame[PivotDFM], timeframe_ohlcv: pt.DataFrame[OHLCV]) \
#         -> list[Tuple[datetime, float]]:
#     exit_bars = [
#         first_exited_bar(start, pivot, timeframe_ohlcv)
#         for start, pivot in timeframe_active_pivots.iterrows()
#     ]
#     return exit_bars


# def passing_time(timeframe_active_pivots: pt.DataFrame[PivotDFM], timeframe_ohlcv: pt.DataFrame[OHLCV]):
#     _exit_bars = exit_bars(timeframe_active_pivots, timeframe_ohlcv)
#     return dict(_exit_bars).keys()


def insert_passing_time(pivots: pt.DataFrame[PivotDFM], base_timeframe_ohlcv: pt.DataFrame[OHLCV]):
    if 'passing_time' not in pivots.columns:
        pivots['passing_time'] = pd.Series(dtype='datetime64[ns, UTC]')

    support_pivots = pivots[pivots['is_resistance'] == False]
    if any(pivots.loc[support_pivots.index, 'external_margin'] > pivots.loc[support_pivots.index, 'internal_margin']):
        pass
    pivots.loc[support_pivots.index, 'passing_time'] = \
        find_crossing(support_pivots, 'external_margin',
                      base_timeframe_ohlcv, 'high', 'right', lambda target, base: target < base,
                      return_both=False,
                      )

    resistance_pivots = pivots[pivots['is_resistance'] == True]
    if any(pivots.loc[resistance_pivots.index, 'external_margin'] < pivots.loc[
        resistance_pivots.index, 'internal_margin']):
        pass
    pivots.loc[resistance_pivots.index, 'passing_time'] = \
        find_crossing(resistance_pivots, 'external_margin',
                      base_timeframe_ohlcv, 'low', 'right', lambda target, base: target > base,
                      return_both=False,
                      )
    valid_passing_pivots = pivots[pivots['passing_time'].notna() & (pivots['passing_time'] < pivots['ttl'])].index
    pivots.loc[valid_passing_pivots, 'deactivated_at'] = pivots.loc[valid_passing_pivots, 'passing_time']


def deactivate_by_ttl(pivots: pt.DataFrame[MultiTimeframePivotDFM], base_timeframe_ohlcv: pt.DataFrame[OHLCV]):
    end = base_timeframe_ohlcv.index.get_level_values(level='date').max()  # todo: test
    ttl_expired = pivots[
        (pivots['deactivated_at'].isna() | ~pivots['deactivated_at'])
        & (pivots['ttl'] < end)
        ].index
    pivots.loc[ttl_expired, 'deactivated_at'] = pivots.loc[ttl_expired, 'ttl']


def duplicate_on_passing_times(pivots: pt.DataFrame['PivotDFM'], base_timeframe_ohlcv: pt.DataFrame[OHLCV],
                               timeframe: str):
    if 'passing_time' in pivots.columns:
        raise AssertionError("'passing_time' in pivots.columns")
    pivots['passing_time'] = pd.Series(dtype='datetime64[ns, UTC]')
    may_have_passing = pivots[pivots['deactivated_at'].isna()]
    while len(may_have_passing) > 0:  # len(old_pivots_with_passing) < pivots[pivots['passing_time'].notna()]:
        insert_passing_time(may_have_passing, base_timeframe_ohlcv)
        if not may_have_passing['passing_time'].dropna().is_unique:
            raise AssertionError("not may_have_passing['passing_time'].dropna().is_unique")
        passed_pivots = may_have_passing[may_have_passing['passing_time'].notna()]
        pivots.loc[passed_pivots.index, 'passing_time'] = passed_pivots['passing_time']
        may_have_passing = PivotDf.new()
        for pivot_date, pivot_info in passed_pivots.iterrows():
            pivots.loc[pivot_date, 'deactivated_at'] = pivot_info['passing_time']
            pivot_dict = pivot_info.to_dict()
            pivot_dict.update({
                'date': pivot_info['passing_time'],
                'level': pivot_info['level'],
                'is_resistance': not pivot_info['is_resistance'],
                'original_start': pivot_info['original_start'],
                'internal_margin': pivot_info['external_margin'],
                'external_margin': pivot_info['internal_margin'],
                # 'hit': 0,
                'ttl': pivot_info['ttl'],
                'return_end_time': pivot_info['passing_time'],
                'passing_time': pd.NaT,
                'deactivated_at': pd.NaT,
            })
            new_pivot = PivotDf.new(pivot_dict, strict=False)
            may_have_passing = PivotDf.concat(may_have_passing, new_pivot)
            if not may_have_passing.index.is_unique:
                raise AssertionError("not may_have_passing.index.is_unique")
            if not may_have_passing['passing_time'].dropna().is_unique:
                raise AssertionError("not may_have_passing['passing_time'].is_unique")
        for t_index, t_pivot in may_have_passing.iterrows():
            with warnings.catch_warnings():
                warnings.simplefilter(action='ignore', category=FutureWarning)
                pivots.loc[t_index] = t_pivot
        # pivots = MultiTimeframePivotDf.concat(pivots, may_have_passing)
        if not pivots.index.is_unique:
            raise AssertionError("not pivots.index.is_unique")
        if not pivots['passing_time'].dropna().is_unique:
            raise AssertionError("not pivots['passing_time'].is_unique")
    return pivots.sort_index()


# def reactivate_passed_levels(timeframe_active_pivots: pt.DataFrame[PivotDFM],
#                              timeframe_inactive_pivots: pt.DataFrame[PivotDFM],
#                              timeframe_ohlcv: pt.DataFrame[OHLCV]
#                              ) -> Tuple[pt.DataFrame[PivotDFM], pt.DataFrame[PivotDFM]]:
#     # toddo: test
#
#     # timeframe_active_pivots = update_pivot_side(timeframe_active_pivots, timeframe_ohlcv)
#     timeframe_active_pivots['passing_time'] = passing_time(timeframe_active_pivots, timeframe_ohlcv)
#     passed_pivots = timeframe_active_pivots[timeframe_active_pivots['passing_time'].notna()].index
#     timeframe_inactive_pivots: pt.DataFrame[PivotDFM] = (
#         concat(timeframe_inactive_pivots, timeframe_active_pivots.loc[passed_pivots]))
#     timeframe_active_pivots = timeframe_active_pivots.drop(labels=passed_pivots)
#     return timeframe_active_pivots, timeframe_inactive_pivots


def inactivate_3rd_hit(pivots: pt.DataFrame['MultiTimeframePivotDFM'], base_timeframe_ohlcv: pt.DataFrame[OHLCV]):
    pivots = update_hit(pivots, base_timeframe_ohlcv)  # todo: test
    deactivate_by_hit = pivots[pivots['hit'] > config.pivot_number_of_active_hits].index
    pivots.loc[deactivate_by_hit, 'deactivated_at'] = \
        pivots.loc[deactivate_by_hit, f'hit_start_{config.pivot_number_of_active_hits - 1}']


def update_pivot_deactivation(timeframe_pivots: pt.DataFrame['PivotDFM'],
                              base_timeframe_ohlcv: pt.DataFrame[OHLCV], timeframe: str):
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
    timeframe_pivots = duplicate_on_passing_times(timeframe_pivots, base_timeframe_ohlcv, timeframe)
    # reactivate_passed_levels(timeframe_active_pivots, timeframe_inactive_pivots))
    inactivate_3rd_hit(timeframe_pivots, base_timeframe_ohlcv)  # todo: test
    deactivate_by_ttl(timeframe_pivots, base_timeframe_ohlcv)
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


# def get_date_range_of_pivots_ftc():
#     raise NotImplementedError
#     _, end = date_range(config.processing_date_range)
#     first_movement_start = active_pivot_levels['movement_start_time'].dropna().min()


# def update_levels(active_pivot_levels: pt.DataFrame[MultiTimeframePivotDFM],
#                   inactive_pivot_level: pt.DataFrame[MultiTimeframePivotDFM],
#                   archived_pivot_levels: pt.DataFrame[MultiTimeframePivotDFM],
#                   date_range_str: str = None):
#     active_pivot_levels, inactive_pivot_level = update_active_levels(active_pivot_levels, inactive_pivot_level)
#     active_pivot_levels, inactive_pivot_level = update_inactive_levels(active_pivot_levels, inactive_pivot_level)
#     insert_pivot_ftc(active_pivot_levels)


# @measure_time
# def generate_classic_pivots(date_range_str: str = None):
#     """
#     definition of pivot:
#         highest high of every Bullish and lowest low of every Bearish trend. for Trends
#             conditions:
#                 >= 3 atr
#                 >= 1 atr reverse movement after the most significant top
#             index = highest high for Bullish and lowest low for Bearish
#             highest high is not the last peak of Bullish and lowest low is not the last Valley raise a warning log:
#                 timeframe, trend start time (index), time of last top
#                 time and high of highest high in Bullish and time and low of lowest low in Bearish,
#         same color trends >= 3 atr + reverse same color trend >= 1 atr
#             condition:
#
#     pivot information:
#         index: datetime
#     """
#     if date_range_str is None:  # toddo: test
#         date_range_str = config.processing_date_range
#     multi_timeframe_pivots = read_classic_pivots(date_range_str)
#     raise NotImplementedError
#     #
#     # for (pivot_timeframe, pivot_time), pivot_info in multi_timeframe_pivots.sort_index(level='date'):
#     #     multi_timeframe_pivots.loc[(pivot_timeframe, pivot_time), 'is_overlap_of'] = \
#     #         pivot_exact_overlapped(pivot_time, multi_timeframe_pivots)
#     # base_timeframe_ohlcv = read_base_timeframe_ohlcv()
#     # update_pivot_deactivation(multi_timeframe_pivots, base_timeframe_ohlcv)


# def insert_ftc(pivots: pt.DataFrame[AtrMovementPivotDFM],
#                base_patterns: pt.DataFrame[MultiTimeframeBasePattern]):
#     """
#     The FTC is a base pattern in the last 38% of pivot movement price range.
#     The FTC base pattern should start within the movement time range.
#     The base pattern timeframe should be below the pivot timeframe.
#
#     :param base_patterns:
#     :param pivots:
#     :param structure_timeframe_shortlist:
#     :return:
#     """
#     pivots['ftc_high'] = pivots[['movement_start_value', 'level']].max(axis='columns')  # toddo: test
#     pivots['ftc_low'] = pivots[['movement_start_value', 'level']].min(axis='columns')
#     pivots['movement_size'] = pivots['movement_end_value'] - pivots['movement_start_value']
#     pivots['ftc_price_range_start'] = \
#         pivots['movement_end_value'] - (pivots['movement_size'] * config.ftc_price_range_percentage)
#
#     for (timeframe, start), pivot_info in pivots:
#         ftc_base_patterns = base_patterns[
#             (base_patterns.index.get_level_values(level='date') < start)
#             & (base_patterns.index.get_level_values(level='date') > pivots['movement_start_time'])
#             & (base_patterns['internal_high'] <= pivots['ftc_high'])
#             & (base_patterns['internal_low'] >= pivots['ftc_low'])
#             ]
#         if len(ftc_base_patterns) > 0:
#             pivots.loc[(timeframe, start), 'ftc_base_pattern_timeframes'] = \
#                 ftc_base_patterns.index.get_level_values(level='timeframe').tolist()
#             pivots.loc[(timeframe, start), 'ftc_base_pattern_dates'] = \
#                 ftc_base_patterns.index.get_level_values(level='date').tolist()


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


def insert_is_overlap_of(multi_timeframe_pivots: pt.DataFrame[MultiTimeframePivotDFM]):
    """
    find the pivot with maximum timeframe and if there are several pivots with the same maximum timeframe, the first is
    the most significant.
    :param multi_timeframe_pivots:
    :return:
    """
    for mt_index, pivot_info in multi_timeframe_pivots.iterrows():
        lows = multi_timeframe_pivots[['internal_margin', 'external_margin']].min(axis='columns')
        highs = multi_timeframe_pivots[['internal_margin', 'external_margin']].max(axis='columns')
        overlapped_pivots = multi_timeframe_pivots[
            (pivot_info['level'] < highs)
            & (pivot_info['level'] > lows)
            ]
        if len(overlapped_pivots) > 0:
            overlapped_timeframes = overlapped_pivots.index.get_level_values(level='timeframe').unique()
            timeframe = [t for t in config.timeframes[::-1] if t in overlapped_timeframes][0]
            (master_pivot_timeframe, master_pivot_date) = \
                overlapped_pivots[overlapped_pivots['timeframe'] == timeframe].sort_index(level='date').iloc[0].index
            multi_timeframe_pivots.loc[mt_index, 'master_pivot_timeframe'] = master_pivot_timeframe
            multi_timeframe_pivots.loc[mt_index, 'master_pivot_date'] = master_pivot_date


# def pivot_exact_overlapped(pivot_time, multi_timeframe_pivots):
#     # todo: try to merge with update_hit
#     # new_pivots['is_overlap_of'] = None
#     if not len(multi_timeframe_pivots) > 0:
#         return None
#
#     # for pivot_time, pivot in new_pivots.iterrows():
#     overlapping_major_timeframes = \
#         multi_timeframe_pivots[multi_timeframe_pivots.index.get_level_values('date') == pivot_time]
#     if len(overlapping_major_timeframes) > 0:
#         root_pivot = overlapping_major_timeframes[
#             multi_timeframe_pivots['is_overlap_of'].isna()
#         ]
#         if len(root_pivot) == 1:
#             # new_pivots.loc[pivot_time, 'is_overlap_of'] = \
#             return root_pivot.index.get_level_values('timeframe')[0]
#         else:
#             raise Exception(f'Expected to find only one root_pivot but found({len(root_pivot)}):{root_pivot}')
#     # return new_pivots
#     # raise Exception(f'Expected to find a root_pivot but found zero')


def update_hit(timeframe_pivots: pt.DataFrame[PivotDFM], base_timeframe_ohlcv) -> pt.DataFrame[PivotDFM]:
    if 'passing_time' not in timeframe_pivots.columns:  # todo: test
        AssertionError("Expected passing_time be calculated before: "
                       "'passing_time' not in active_timeframe_pivots.columns")
    pivot_indexes = timeframe_pivots.index
    # timeframe_pivots.loc[pivot_indexes, 'boundary_of_hit_start_0'] = \
    #     nearest_match(needles=pivot_indexes, reference=base_timeframe_ohlcv.index, direction='backward', shift=1)
    for n in range(config.pivot_number_of_active_hits):
        timeframe_pivots[f'boundary_of_hit_start_{n}'] =  pd.Series(dtype='datetime64[ns, UTC]')
        if n == 0:
            timeframe_pivots.loc[pivot_indexes, 'boundary_of_hit_start_0'] = \
                nearest_match(needles=timeframe_pivots['return_end_time'].tolist(),
                              reference=base_timeframe_ohlcv.index,
                              direction='backward', shift=1)
        else:
            timeframe_pivots.loc[pivot_indexes, f'boundary_of_hit_start_{n}'] = \
                nearest_match(needles=timeframe_pivots.loc[pivot_indexes, f'boundary_of_hit_end_{n - 1}'],
                              reference=base_timeframe_ohlcv.index, direction='backward', shift=1)
        # timeframe_pivots[f'hit_start_{n}'] = pd.Series(dtype='datetime64[ns, UTC]')
        timeframe_pivots.loc[pivot_indexes][f'hit_start_{n}'] = (
            insert_hits(timeframe_pivots.loc[pivot_indexes], f'boundary_of_hit_start_{n}', n, \
                        base_timeframe_ohlcv, 'start'))[f'hit_start_{n}']

        timeframe_pivots.loc[pivot_indexes, f'boundary_of_hit_end_{n}'] = \
            nearest_match(needles=timeframe_pivots.loc[pivot_indexes, f'boundary_of_hit_start_{n}'],
                          reference=base_timeframe_ohlcv.index, direction='backward', shift=1)
        # timeframe_pivots[f'hit_end_{n}'] = pd.Series(dtype='datetime64[ns, UTC]')
        timeframe_pivots.loc[pivot_indexes][f'hit_end_{n}'] = (
            insert_hits(timeframe_pivots.loc[pivot_indexes], f'boundary_of_hit_end_{n}', n, \
                        base_timeframe_ohlcv, 'end'))[f'hit_end_{n}']
        invalid_hit_ends = timeframe_pivots[timeframe_pivots.loc[pivot_indexes, f'hit_end_{n}'] <
                                            timeframe_pivots.loc[pivot_indexes, f'passed']]
        timeframe_pivots.loc[invalid_hit_ends, f'hit_end_{n}'] = pd.NaT
        timeframe_pivots.loc[timeframe_pivots[f'hit_end_{n}'].notna(), 'hit'] = n
        pivot_indexes = timeframe_pivots[timeframe_pivots[f'hit_end_{n}'].notna()]

    return timeframe_pivots


def insert_hits(active_timeframe_pivots: pt.DataFrame[PivotDFM], hit_boundary_column: str, n: int,
                base_timeframe_ohlcv: pt.DataFrame[OHLCV],
                side: Literal['start', 'end']):
    t_df: pd.DataFrame = active_timeframe_pivots.reset_index(level='date')  # todo: test
    t_df.rename(columns={'date': 'old_date'}, inplace=True)
    t_df.rename(columns={hit_boundary_column: 'date'}, inplace=True)
    t_df.set_index('date', inplace=True)  # todo: test
    resistance_indexes = t_df[t_df['is_resistance'] == True].index
    t_df.loc[resistance_indexes, f'hit_{side}_{n}'] = \
        insert_hit(t_df.loc[resistance_indexes], n, base_timeframe_ohlcv, side, 'Resistance') \
            [f'hit_{side}_{n}']
    t_support_indexes = t_df[t_df['is_resistance'] == False].index
    t_df.loc[t_support_indexes, f'hit_{side}_{n}'] = \
        insert_hit(t_df.loc[t_support_indexes], n, base_timeframe_ohlcv, side, 'Support') \
            [f'hit_{side}_{n}']
    t_df.reset_index(inplace=True)
    t_df.rename(columns={'date': hit_boundary_column}, inplace=True)
    t_df.rename(columns={'old_date': 'date'}, inplace=True)
    t_df.set_index('date', inplace=True)
    return t_df


def insert_hit(pivots: pt.DataFrame[PivotDFM], n: int, base_timeframe_ohlcv: pt.DataFrame[OHLCV],
               side: Literal['start', 'end'], pivot_type: Literal['Resistance', 'Support']):
    if pivot_type == 'Resistance':  # todo: test
        def gt(target, base):
            return target >= base

        def lt(target, base):
            return target > base
    else:  # pivot_type == 'Support'
        def gt(target, base):
            return target <= base

        def lt(target, base):
            return target < base
    if side == 'start':
        if not pivots.index.is_unique:
            pass
        pivots[f'hit_start_{n}'] = \
            find_crossing(pivots, 'internal_margin', base_timeframe_ohlcv, 'high',
                          'right', gt, return_both=False, )
    else:
        if not pivots.index.is_unique:
            pass
        pivots[f'hit_end_{n}'] = \
            find_crossing(pivots, 'internal_margin', base_timeframe_ohlcv, 'high',
                          'right', lt, return_both=False, )

    # if pivots['is_resistance'].isna().any():
    #     raise AssertionError("active_timeframe_pivots['is_resistance'].isna().any()")


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
@measure_time
def insert_pivot_info(timeframe_pivots: pt.DataFrame['PivotDFM'], ohlcva: pt.DataFrame[OHLCVA],
                      structure_timeframe_shortlist: List[str], base_timeframe_ohlcv: pt.DataFrame[OHLCV],
                      timeframe: str):
    insert_pivot_type_n_level(timeframe_pivots)
    # if structure_timeframe_shortlist is None:
    #     structure_timeframe_shortlist = config.structure_timeframes[::-1]
    # multi_timeframe_pivots = MultiTimeframeAtrMovementPivotDf.new()
    # # for timeframe in pivots.index.get_level_values('timeframe').intersection(structure_timeframe_shortlist):
    # # timeframe_ohlcva = ohlcva  # single_timeframe(mt_ohlcva, timeframe)
    # timeframe_pivots = pivots[pivots.index.get_level_values(level='timeframe') == timeframe]
    # timeframe_pivots = timeframe_pivots.reset_index(level='timeframe')

    timeframe_pivots['original_start'] = timeframe_pivots.index

    timeframe_resistance_pivots = timeframe_pivots[
        timeframe_pivots['peak_or_valley'] == TopTYPE.PEAK.value].index
    timeframe_pivots.loc[timeframe_resistance_pivots, ['internal_margin', 'external_margin']] = \
        pivot_margins(timeframe_pivots.loc[timeframe_resistance_pivots], _type=TopTYPE.PEAK,
                      pivot_peaks_or_valleys=timeframe_pivots.loc[timeframe_resistance_pivots],
                      candle_body_source=ohlcva,
                      # timeframe_pivots.loc[timeframe_resistance_pivots],
                      timeframe=timeframe,
                      internal_margin_atr=ohlcva,
                      breakout_margin_atr=ohlcva)[['internal_margin', 'external_margin']]

    timeframe_support_pivots = timeframe_pivots[
        timeframe_pivots['peak_or_valley'] == TopTYPE.VALLEY.value].index
    timeframe_pivots.loc[timeframe_support_pivots, ['internal_margin', 'external_margin', ]] = \
        pivot_margins(timeframe_pivots.loc[timeframe_support_pivots], _type=TopTYPE.VALLEY,
                      pivot_peaks_or_valleys=timeframe_pivots.loc[timeframe_support_pivots],
                      candle_body_source=ohlcva,
                      # timeframe_pivots.loc[timeframe_support_pivots],
                      timeframe=timeframe,
                      internal_margin_atr=ohlcva,
                      breakout_margin_atr=ohlcva)[['internal_margin', 'external_margin']]

    timeframe_pivots['ttl'] = timeframe_pivots.index + level_ttl(timeframe)
    timeframe_pivots['deactivated_at'] = pd.Series(dtype='datetime64[ns, UTC]')
    timeframe_pivots['archived_at'] = pd.Series(dtype='datetime64[ns, UTC]')
    timeframe_pivots['master_pivot_timeframe'] = pd.Series(dtype='str')
    timeframe_pivots['master_pivot_date'] = pd.Series(dtype='datetime64[ns, UTC]')
    timeframe_pivots['hit'] = 0
    update_pivot_deactivation(timeframe_pivots, base_timeframe_ohlcv, timeframe)  # todo: test
    # insert_ftc(timeframe_pivots, structure_timeframe_shortlist)
    timeframe_pivots = AtrMovementPivotDf.cast_and_validate(timeframe_pivots)
    # timeframe_pivots['timeframe'] = timeframe
    # timeframe_pivots = timeframe_pivots.set_index('timeframe', append=True)
    # timeframe_pivots = timeframe_pivots.swaplevel()
    #     multi_timeframe_pivots: pt.DataFrame[MultiTimeframeAtrMovementPivotDFM] = \
    #         MultiTimeframeAtrMovementPivotDf.concat(multi_timeframe_pivots, timeframe_pivots)
    # pivots = MultiTimeframeAtrMovementPivotDf.cast_and_validate(multi_timeframe_pivots)

    return timeframe_pivots


def insert_pivot_type_n_level(pivots: pt.DataFrame[MultiTimeframePivotDFM]):
    if 'is_resistance' not in pivots.columns:
        pivots['is_resistance'] = pd.Series(dtype=bool)
    resistance_pivots = pivots[pivots['peak_or_valley'] == TopTYPE.PEAK.value].index
    pivots.loc[resistance_pivots, 'level'] = pivots.loc[resistance_pivots, 'high']
    pivots.loc[resistance_pivots, 'is_resistance'] = True
    support_pivots = pivots[pivots['peak_or_valley'] == TopTYPE.VALLEY.value].index
    pivots.loc[support_pivots, 'level'] = pivots.loc[support_pivots, 'low']
    pivots.loc[support_pivots, 'is_resistance'] = False
