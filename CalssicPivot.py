import pandas as pd

from BullBearSidePivot import generate_multi_timeframe_bull_bear_side_pivots
from ColorTrend import generate_multi_timeframe_color_trend_pivots
from Config import config
from DataPreparation import single_timeframe
from PeakValley import read_multi_timeframe_peaks_n_valleys


def level_hit_count():
    """
    number of pivots:
        after activation time
        between inner_margin and outer_margin of level
    if hit_count > 2: mark level as inactive.
    if price moved from inner_margin to outer_margin: reset hit count to 0 and mark level as active

    :return:
    """
    raise Exception('Not implemented')


def update_active_levels():
    """
        hit_count = number of pivots:
            after activation time
            between inner_margin and outer_margin of level
        if hit_count > 2: mark level as inactive.
    """
    raise Exception('Not implemented')


def archive_cold_levels():
    """
            archive levels which have been inactive for more than 16^2 intervals
    """
    raise Exception('Not implemented')


def reactivated_passed_levels():
    """
        if price moved from inner_margin to outer_margin: reset hit count to 0 and mark level as active
        for any of inactive levels:
            find boundaries starts after level has been inactivated
            merge adjacent boundaries and use highest high and lowest low for merged boundaries.
            if any boundary which level is between boundary's low and high:
                reactivate level
    :return:
    """
    raise Exception('Not implemented')


def update_inactive_levels():
    """
        archive levels which have been inactive for more than 16^2 intervals
        if price moved from inner_margin to outer_margin: reset hit count to 0 and mark level as active
    :return:
    """
    # todo: test update_inactive_levels
    archive_cold_levels()
    reactivated_passed_levels()


def update_levels():
    """
    hit_count = number of pivots:
        after activation time
        between inner_margin and outer_margin of level
    if hit_count > 2: mark level as inactive.
    if price moved from inner_margin to outer_margin: reset hit count to 0 and mark level as active

    :return:
    """
    # todo: test update_levels
    update_active_levels()
    update_inactive_levels()


def pivot_exact_overlapped(pivot_time, multi_timeframe_pivots):
    # new_pivots['overlapped_with_major_timeframe'] = None
    if not len(multi_timeframe_pivots) > 0:
        return None

    # for pivot_time, pivot in new_pivots.iterrows():
    overlapping_major_timeframes = \
        multi_timeframe_pivots[multi_timeframe_pivots.index.get_level_values('date') == pivot_time]
    if len(overlapping_major_timeframes) > 0:
        root_pivot = overlapping_major_timeframes[
            multi_timeframe_pivots['overlapped_with_major_timeframe'].isna()
        ]
        if len(root_pivot) == 1:
            # new_pivots.loc[pivot_time, 'overlapped_with_major_timeframe'] = \
            return root_pivot.index.get_level_values('timeframe')[0]
        else:
            raise Exception(f'Expected to find only one root_pivot but found({len(root_pivot)}):{root_pivot}')
    # return new_pivots
    raise Exception(f'Expected to find a root_pivot but found zero')


def pivot_hit(pivot_time, pivot_info, multi_timeframe_pivots):
    active_pivots = multi_timeframe_pivots[multi_timeframe_pivots['deactivation_time'] > pivot_time]
    margin_top = active_pivots['internal_margin','external_margin'].min(axis='column')
    margin_bottom = active_pivots['internal_margin','external_margin'].max(axis='column')
    hit_pivots = active_pivots[
        (pivot_info['level'] > margin_bottom)
        & (pivot_info['level'] < margin_top)
    ]
    if not len(hit_pivots) > 0:
        return None
    original_pivot = hit_pivots[hit_pivots['is_a_heat'].isnull()]
    return hit_pivots.index

def anti_pattern_tops_pivots(date_range_str):
    _multi_timeframe_peaks_n_valleys = read_multi_timeframe_peaks_n_valleys(date_range_str)
    for _timeframe in config.structure_timeframes[::-1][1:]:
        _pivots = single_timeframe(_multi_timeframe_peaks_n_valleys, _timeframe)
        _pivots['level'] = _pivots['']
        _pivots = _pivots.loc[:,None]



def generate_multi_timeframe_anti_pattern_tops_pivots():
    # tops of timeframe which the timeframe is its pattern timeframe
    for _timeframe in config.structure_timeframes[::-1][1:]:

    raise Exception('Not implemented')


def generate_multi_timeframe_pivot_levels(multi_timeframe_ohlca, multi_timeframe_peaks_n_valleys: pd.DataFrame):
    """
    definition of pivot:
        highest high of every Bullish and lowest low of every Bearish trend. for Trends
            conditions:
                >= 3 ATR
                >= 1 ATR reverse movement after the most significant top
            index = highest high for Bullish and lowest low for Bearish
            highest high is not the last peak of Bullish and lowest low is not the last Valley raise a warning log:
                timeframe, trend start time (index), time of last top
                time and high of highest high in Bullish and time and low of lowest low in Bearish,
        same color trends >= 3 ATR + reverse same color trend >= 1 ATR
            condition:

    pivot information:
        index: datetime
    """
    """
        todo: test timeframe of SR as:
            timeframe of trend
            timeframe of top
    """
    generate_multi_timeframe_bull_bear_side_pivots()
    generate_multi_timeframe_color_trend_pivots()
    generate_multi_timeframe_anti_pattern_tops_pivots()
    update_levels()
