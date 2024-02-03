from datetime import datetime
from enum import Enum
from typing import Literal

import pandas as pd
from pandera import typing as pt

from BullBearSidePivot import read_multi_timeframe_bull_bear_side_pivots
from Config import config, TopTYPE
from PanderaDFM.OHLCV import OHLCV
from PanderaDFM.OHLCVA import OHLCVA
from PanderaDFM.Pivot import MultiTimeframePivotDFM
from PanderaDFM.Pivot2 import Pivot2DFM, Pivot2Df
from PeakValley import insert_crossing2
from PeakValleyPivots import read_multi_timeframe_major_times_top_pivots
from PivotsHelper import pivot_margins, level_ttl
from helper.data_preparation import concat, nearest_match


class LevelDirection(Enum):
    Support = 'support'
    Resistance = 'resistance'


def insert_passing_time(pivots: pt.DataFrame[Pivot2DFM], ohlcv: pt.DataFrame[OHLCV]):
    if 'passing_time' not in pivots.columns:
        pivots['passing_time'] = pd.Series(dtype='datetime64[ns, UTC]')
    support_pivots = pivots[~(pivots['is_resistance'].astype(bool))]
    if any(pivots.loc[support_pivots.index, 'external_margin'] >
           pivots.loc[support_pivots.index, 'internal_margin']):
        raise AssertionError(
            "any(pivots.loc[support_pivots.index, 'external_margin'] > pivots.loc[support_pivots.index, "
            "'internal_margin'])")
    pivots.loc[support_pivots.index, 'passing_time'] = \
        insert_crossing2(base=support_pivots, base_target_column='external_margin',
                         target=ohlcv, target_compare_column='high', direction='right',
                         more_significant=lambda target, base: target < base,
                         )['right_crossing_time']

    resistance_pivots = pivots[pivots['is_resistance'].astype(bool)]
    if any(pivots.loc[resistance_pivots.index, 'external_margin'] <
           pivots.loc[resistance_pivots.index, 'internal_margin']):
        raise AssertionError("any(pivots.loc[resistance_pivots.index, 'external_margin'] < "
                             "pivots.loc[resistance_pivots.index, 'internal_margin'])")
    pivots.loc[resistance_pivots.index, 'passing_time'] = \
        insert_crossing2(base=resistance_pivots, base_target_column='external_margin',
                         target=ohlcv, target_compare_column='low', direction='right',
                         more_significant=lambda target, base: target > base,
                         )['right_crossing_time']

    valid_passing_pivots = pivots[pivots['passing_time'].notna() & (pivots['passing_time'] < pivots['ttl'])].index
    pivots.loc[valid_passing_pivots, 'deactivated_at'] = pivots.loc[valid_passing_pivots, 'passing_time']
    return pivots


def deactivate_by_ttl(pivots: pt.DataFrame[Pivot2DFM], end: datetime):
    deactivated_after_ttl = pivots[pivots['deactivated_at'].notna() & (pivots['deactivated_at'] > pivots['ttl'])].index
    if len(deactivated_after_ttl) > 0:
        pass  # todo: test
    pivots.loc[deactivated_after_ttl, 'deactivated_at'] = pivots.loc[deactivated_after_ttl, 'ttl']
    ttl_expired = pivots[
        pivots['deactivated_at'].isna()
        & (pivots['ttl'] < end)
        ].index
    if len(ttl_expired) > 0:
        pass  # todo: test
    pivots.loc[ttl_expired, 'deactivated_at'] = pivots.loc[ttl_expired, 'ttl']


def deactivate_on_passing_times(pivots: pt.DataFrame['Pivot2DFM'], ohlcv: pt.DataFrame[OHLCV], ):
    if 'passing_time' not in pivots.columns:
        pivots['passing_time'] = pd.Series(dtype='datetime64[ns, UTC]')
    may_have_passing = pivots[pivots['deactivated_at'].isna()]
    may_have_passing.loc[:, ['passing_time', 'deactivated_at']] = \
        insert_passing_time(may_have_passing, ohlcv)[['passing_time', 'deactivated_at']]
    passed_pivots = may_have_passing[may_have_passing['passing_time'].notna()]
    pivots.loc[passed_pivots.index, 'passing_time'] = passed_pivots['passing_time']
    pivots.loc[passed_pivots.index, 'deactivated_at'] = passed_pivots['passing_time']


def duplicate_on_passing_times(pivots: pt.DataFrame['Pivot2DFM'], ohlcv: pt.DataFrame[OHLCV], ):
    if 'passing_time' in pivots.columns:
        raise AssertionError("'passing_time' in pivots.columns")
    if 'passing_time' not in pivots.columns:
        pivots['passing_time'] = pd.Series(dtype='datetime64[ns, UTC]')
    may_have_passing = pivots[pivots['deactivated_at'].isna()]
    while len(may_have_passing) > 0:  # len(old_pivots_with_passing) < pivots[pivots['passing_time'].notna()]:
        insert_passing_time(may_have_passing, ohlcv)
        passed_pivots = may_have_passing[may_have_passing['passing_time'].notna()]
        pivots.loc[passed_pivots.index, ['passing_time', 'deactivated_at']] = \
            passed_pivots[['passing_time', 'deactivated_at']]
        may_have_passing = Pivot2Df.new()
        for (pivot_date, pivot_original_start), pivot_info in passed_pivots.iterrows():
            new_pivot_dict = pivot_info.to_dict()
            new_pivot_dict.update({
                'date': pivot_info['passing_time'],
                'level': pivot_info['level'],
                'is_resistance': not bool(pivot_info['is_resistance']),
                'original_start': pivot_original_start,
                'internal_margin': pivot_info['external_margin'],
                'external_margin': pivot_info['internal_margin'],
                'hit': 0,
                'ttl': pivot_info['ttl'],
                'return_end_time': pivot_info['passing_time'],
                'passing_time': pd.NaT,
                'deactivated_at': pd.NaT,
            })
            new_pivot = Pivot2Df.new(new_pivot_dict, strict=False)
            # pivots.at[new_pivot.index] = new_pivot.iloc[0]
            may_have_passing = Pivot2Df.concat(may_have_passing, new_pivot)
            pivots = Pivot2Df.concat(pivots, new_pivot)
        # for t_index, t_pivot in may_have_passing.iterrows():
        #     # with warnings.catch_warnings():
        #     # warnings.simplefilter(action='ignore', category=FutureWarning)
        #     pivots.loc[t_index] = t_pivot.dropna()
        #     pass
    return pivots.sort_index()


def inactivate_3rd_hit(pivots: pt.DataFrame['Pivot2DFM'], ohlcv: pt.DataFrame[OHLCV]):
    pivots = update_hit(pivots, ohlcv)
    deactivate_by_hit = pivots[pivots[f'hit_end_{config.pivot_number_of_active_hits}'].notna()].index
    pivots.loc[deactivate_by_hit, 'deactivated_at'] = \
        pivots.loc[deactivate_by_hit, f'hit_end_{config.pivot_number_of_active_hits}']


def update_pivot_deactivation(timeframe_pivots: pt.DataFrame['Pivot2DFM'], timeframe: str, ohlcv: pt.DataFrame[OHLCV]):
    """
        hit_count = number of pivots:
            after activation time
            between inner_margin and outer_margin of level
        if hit_count > 2: mark level as inactive.
    """
    timeframe_pivots = duplicate_on_passing_times(timeframe_pivots, ohlcv)
    inactivate_3rd_hit(timeframe_pivots, ohlcv)
    end = ohlcv.index.get_level_values(level='date').max()
    deactivate_by_ttl(timeframe_pivots, end)
    return timeframe_pivots


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


def update_hit(timeframe_pivots: pt.DataFrame[Pivot2DFM], ohlcv) -> pt.DataFrame[Pivot2DFM]:
    if 'passing_time' not in timeframe_pivots.columns:  # todo: test
        AssertionError("Expected passing_time be calculated before: "
                       "'passing_time' not in active_timeframe_pivots.columns")
    may_have_hit = timeframe_pivots.index
    if 'date' not in timeframe_pivots.columns:
        timeframe_pivots['start_date'] = timeframe_pivots.index.get_level_values(level='date')
    # finding the first exit from pivot margins = hit_end_0
    timeframe_pivots[f'boundary_of_hit_end_0'] = \
        nearest_match(needles=timeframe_pivots.loc[may_have_hit, ['return_end_time', 'start_date']].max(axis='columns') \
                      .tolist(), reference=ohlcv.index, direction='backward', shift=1)
    timeframe_pivots.drop(columns='start_date', inplace=True)
    try:
        timeframe_pivots[f'hit_end_0'] = (
            insert_hits(timeframe_pivots, f'boundary_of_hit_end_0', 0,
                        ohlcv, 'end'))[f'hit_end_0']
    except Exception as e:
        nop = 1
        raise e
    for n in range(1, config.pivot_number_of_active_hits + 1):
        timeframe_pivots[f'boundary_of_hit_start_{n}'] = pd.Series(dtype='datetime64[ns, UTC]')
        timeframe_pivots[f'boundary_of_hit_end_{n}'] = pd.Series(dtype='datetime64[ns, UTC]')
        # filter for next iteration
        may_have_hit = timeframe_pivots[timeframe_pivots[f'hit_end_{n - 1}'].notna()].index
        # finding hit n start
        timeframe_pivots.loc[may_have_hit, f'boundary_of_hit_start_{n}'] = \
            nearest_match(needles=timeframe_pivots.loc[may_have_hit, f'hit_end_{n - 1}'],
                          reference=ohlcv.index, direction='backward', shift=1)
        timeframe_pivots.loc[may_have_hit, f'hit_start_{n}'] = (
            insert_hits(timeframe_pivots.loc[may_have_hit], f'boundary_of_hit_start_{n}', n,
                        ohlcv, 'start'))[f'hit_start_{n}']
        invalid_hit_starts = timeframe_pivots[timeframe_pivots[f'hit_start_{n}'] >
                                              timeframe_pivots['passing_time']].index
        timeframe_pivots.loc[invalid_hit_starts, f'hit_start_{n}'] = pd.NaT
        # finding hit n end
        started_pivots = timeframe_pivots[timeframe_pivots[f'hit_start_{n}'].notna()].index
        timeframe_pivots.loc[started_pivots, f'boundary_of_hit_end_{n}'] = \
            nearest_match(needles=timeframe_pivots.loc[started_pivots, f'hit_start_{n}'],
                          reference=ohlcv.index, direction='backward', shift=1)
        timeframe_pivots.loc[started_pivots, f'hit_end_{n}'] = (
            insert_hits(timeframe_pivots.loc[started_pivots], f'boundary_of_hit_end_{n}', n,
                        ohlcv, 'end'))[f'hit_end_{n}']
        invalid_hit_ends = timeframe_pivots[timeframe_pivots[f'hit_end_{n}'] >
                                            timeframe_pivots['passing_time']].index
        timeframe_pivots.loc[invalid_hit_ends, f'hit_end_{n}'] = pd.NaT
        timeframe_pivots.loc[timeframe_pivots[f'hit_end_{n}'].notna(), 'hit'] = n + 1
    return timeframe_pivots


# @measure_time
def insert_hits(active_timeframe_pivots: pt.DataFrame[Pivot2DFM], hit_boundary_column: str, n: int,
                ohlcv: pt.DataFrame[OHLCV],
                side: Literal['start', 'end']):
    t_df = active_timeframe_pivots.drop(columns=['date', 'start_date'], errors='ignore')
    t_df: pd.DataFrame = t_df.reset_index()
    t_df.rename(columns={'date': 'start_date'}, inplace=True)
    t_df.rename(columns={hit_boundary_column: 'date'}, inplace=True)
    t_df.set_index('date', inplace=True)
    if n == 2:
        pass
    resistance_start_dates = t_df.loc[t_df['is_resistance'], 'start_date'].tolist()
    t_df.loc[t_df['start_date'].isin(resistance_start_dates), f'hit_{side}_{n}'] = \
        insert_hit(t_df[t_df['start_date'].isin(resistance_start_dates)], n, ohlcv, side, 'Resistance') \
            [f'hit_{side}_{n}']

    support_start_dates = t_df.loc[t_df['is_resistance'] == False, 'start_date'].tolist()
    t_df.loc[t_df['start_date'].isin(support_start_dates), f'hit_{side}_{n}'] = \
        insert_hit(t_df.loc[t_df['start_date'].isin(support_start_dates)], n, ohlcv, side, 'Support') \
            [f'hit_{side}_{n}']

    t_df.reset_index(inplace=True)
    t_df.rename(columns={'date': hit_boundary_column}, inplace=True)
    t_df.rename(columns={'start_date': 'date'}, inplace=True)
    if 'original_start' in t_df.columns:
        t_df.set_index(['date', 'original_start'], inplace=True)
    else:
        t_df.set_index('date', inplace=True)
    return t_df


def insert_hit(pivots: pt.DataFrame[Pivot2DFM], n: int, ohlcv: pt.DataFrame[OHLCV],
               side: Literal['start', 'end'], pivot_type: Literal['Resistance', 'Support']):
    if pivot_type == 'Resistance':
        high_low = 'high'

        def gt(target, base):
            return target >= base

        def lt(target, base):
            return target < base
    else:  # pivot_type == 'Support'
        high_low = 'low'

        def gt(target, base):
            return target <= base

        def lt(target, base):
            return target > base
    pivots = pivots.copy()
    if side == 'start':
        pivots.loc[:, f'hit_start_{n}'] = \
            insert_crossing2(base=pivots, base_target_column='internal_margin', target=ohlcv,
                             target_compare_column=high_low,
                             direction='right', more_significant=gt, )['right_crossing_time']
    else:
        pivots.loc[:, f'hit_end_{n}'] = \
            insert_crossing2(base=pivots, base_target_column='internal_margin', target=ohlcv,
                             target_compare_column=high_low,
                             direction='right', more_significant=lt, )['right_crossing_time']
    return pivots


# @measure_time
def insert_pivot_info(timeframe_pivots: pt.DataFrame['Pivot2DFM'], ohlcva: pt.DataFrame[OHLCVA], timeframe: str):
    insert_pivot_type_n_level(timeframe_pivots)

    timeframe_pivots['original_start'] = timeframe_pivots.index
    timeframe_pivots.set_index('original_start', append=True, inplace=True)

    timeframe_resistance_pivots = timeframe_pivots[
        timeframe_pivots['peak_or_valley'] == TopTYPE.PEAK.value].index
    timeframe_pivots.loc[timeframe_resistance_pivots, ['internal_margin', 'external_margin']] = \
        pivot_margins(timeframe_pivots.loc[timeframe_resistance_pivots], _type=TopTYPE.PEAK,
                      pivot_peaks_or_valleys=timeframe_pivots.loc[timeframe_resistance_pivots],
                      candle_body_source=ohlcva, breakout_margin_atr=ohlcva)[['internal_margin', 'external_margin']]

    timeframe_support_pivots = timeframe_pivots[
        timeframe_pivots['peak_or_valley'] == TopTYPE.VALLEY.value].index
    timeframe_pivots.loc[timeframe_support_pivots, ['internal_margin', 'external_margin', ]] = \
        pivot_margins(timeframe_pivots.loc[timeframe_support_pivots], _type=TopTYPE.VALLEY,
                      pivot_peaks_or_valleys=timeframe_pivots.loc[timeframe_support_pivots], candle_body_source=ohlcva,
                      breakout_margin_atr=ohlcva)[['internal_margin', 'external_margin']]

    timeframe_pivots['ttl'] = timeframe_pivots.index.get_level_values(level='date') + level_ttl(timeframe)
    timeframe_pivots['deactivated_at'] = pd.Series(dtype='datetime64[ns, UTC]')
    timeframe_pivots['archived_at'] = pd.Series(dtype='datetime64[ns, UTC]')
    timeframe_pivots['master_pivot_timeframe'] = pd.Series(dtype='str')
    timeframe_pivots['master_pivot_date'] = pd.Series(dtype='datetime64[ns, UTC]')
    timeframe_pivots['hit'] = 0
    # insert_ftc(timeframe_pivots, structure_timeframe_shortlist)
    return timeframe_pivots


def insert_pivot_type_n_level(pivots: pt.DataFrame[Pivot2DFM]):
    if 'is_resistance' not in pivots.columns:
        pivots['is_resistance'] = pd.Series(dtype=bool)
    resistance_pivots = pivots[pivots['peak_or_valley'] == TopTYPE.PEAK.value].index
    pivots.loc[resistance_pivots, 'level'] = pivots.loc[resistance_pivots, 'high']
    pivots.loc[resistance_pivots, 'is_resistance'] = True
    support_pivots = pivots[pivots['peak_or_valley'] == TopTYPE.VALLEY.value].index
    pivots.loc[support_pivots, 'level'] = pivots.loc[support_pivots, 'low']
    pivots.loc[support_pivots, 'is_resistance'] = False
