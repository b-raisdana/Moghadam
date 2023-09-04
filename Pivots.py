import datetime

import pandas as pd
from pandera import typing as pt

# from CalssicPivot import update_inactive_levels
from Config import config
from Model.MultiTimeframePivot import MultiTimeframePivot


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


def level_ttl(timeframe) -> datetime.timedelta:
    return 256 * pd.to_timedelta(timeframe)


def update_hit(level_index, level_info, multi_timeframe_pivots: pt.DataFrame[MultiTimeframePivot]) \
        -> pt.DataFrame[MultiTimeframePivot]:
    """
    number of pivots:
        after activation time
        between inner_margin and outer_margin of level
    if hit_count > 2: mark level as inactive.
    if price moved from inner_margin to outer_margin: reset hit count to 0 and mark level as active

    :return:
    """
    # multi_timeframe_pivots = update_inactive_levels(multi_timeframe_pivots)
    active_multi_timeframe_pivots = multi_timeframe_pivots[multi_timeframe_pivots['deactivated_at'].isnull()]
    pivots_low_margin = active_multi_timeframe_pivots['internal_margin', 'external_margin'].min()
    pivots_high_margin = active_multi_timeframe_pivots['internal_margin', 'external_margin'].max()
    overlapping_pivots = active_multi_timeframe_pivots[
        (level_info['level'] >= pivots_low_margin)
        & (level_info['level'] <= pivots_high_margin)
        ]
    root_overlapping_pivots = overlapping_pivots[overlapping_pivots['is_overlap_of'].isnull()].sort_index(level='date')
    root_overlapping_pivot = root_overlapping_pivots[-1]
    active_multi_timeframe_pivots.loc[level_index, 'is_overlap_of'] = root_overlapping_pivot.index
    active_multi_timeframe_pivots.loc[root_overlapping_pivot.index, 'hit'] += 1
    if root_overlapping_pivot['hit'] > config.LEVEL_MAX_HIT:
        active_multi_timeframe_pivots.loc[root_overlapping_pivot.index, 'deactivated_at'] = level_index

    return multi_timeframe_pivots
