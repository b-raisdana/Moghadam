import pandas as pd
from pandera import typing as pt

import helper
from BullBearSidePivot import read_multi_timeframe_bull_bear_side_pivots
from Model.MultiTimeframeOHLC import OHLCV
from Model.MultiTimeframePivot import MultiTimeframePivot
from PeakValleyPivots import read_multi_timeframe_top_pivots
from Pivots import pivot_exact_overlapped, update_hit
from helper import measure_time


def update_active_levels(multi_timeframe_pivots: pt.DataFrame[MultiTimeframePivot]) \
        -> pt.DataFrame[MultiTimeframePivot]:
    """
        hit_count = number of pivots:
            after activation time
            between inner_margin and outer_margin of level
        if hit_count > 2: mark level as inactive.
    """
    for pivot_index, pivot_info in multi_timeframe_pivots:
        multi_timeframe_pivots = update_hit(pivot_index, pivot_info, multi_timeframe_pivots)
    return multi_timeframe_pivots


def reactivated_passed_levels(_time, ohlc: pt.DataFrame[OHLCV],
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
        filtered_ohlc = ohlc[inactive_pivot_index:_time]

    raise Exception('Not implemented')


def archive_cold_levels(_time, multi_timeframe_pivots: pt.DataFrame[MultiTimeframePivot]) \
        -> pt.DataFrame[MultiTimeframePivot]:
    for pivot_time, pivot_info in multi_timeframe_pivots.iterrows():
        if _time > pivot_info['ttl']:
            multi_timeframe_pivots.loc[pivot_time, 'archived_at'] = pivot_info['ttl']
    return multi_timeframe_pivots


def update_inactive_levels(update__time, ohlc: pt.DataFrame[OHLCV],
                           multi_timeframe_pivots: pt.DataFrame[MultiTimeframePivot]) \
        -> pt.DataFrame[MultiTimeframePivot]:
    """
        archive levels which have been inactive for more than 16^2 intervals
        if price moved from inner_margin to outer_margin: reset hit count to 0 and mark level as active
    :return:
    """
    # todo: test update_inactive_levels
    multi_timeframe_pivots = archive_cold_levels(update__time, multi_timeframe_pivots)
    multi_timeframe_pivots = reactivated_passed_levels(multi_timeframe_pivots)

    return multi_timeframe_pivots


def update_levels():
    """
    hit_count = number of pivots:
        after activation time
        between inner_margin and outer_margin of level
    if hit_count > 2: mark level as inactive.
    if price moved from inner_margin to outer_margin: reset hit count to 0 and mark level as active

    :return:
    """
    raise Exception('Not implemented')
    # todo: test update_levels
    for time in range(start_time + pd.to_datetime(timeframe), end_time, pd.to_datetime(timeframe)):
        pass
    update_active_levels()
    update_inactive_levels()


@measure_time
def generate_multi_timeframe_pivot_levels(date_range_str: str = None):
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
    if date_range_str is None:
        date_range_str = config.under_process_date_range
    multi_timeframe_bull_bear_side_pivots = read_multi_timeframe_bull_bear_side_pivots(date_range_str)
    multi_timeframe_anti_pattern_tops_pivots = read_multi_timeframe_top_pivots(date_range_str)
    # multi_timeframe_color_trend_pivots = read_multi_timeframe_color_trend_pivots()
    multi_timeframe_pivots = pd.concat([
        multi_timeframe_bull_bear_side_pivots,
        # multi_timeframe_color_trend_pivots,
        multi_timeframe_anti_pattern_tops_pivots,
    ])
    for (pivot_timeframe, pivot_time), pivot_info in multi_timeframe_pivots.sort_index(level='date'):
        multi_timeframe_pivots.loc[(pivot_timeframe, pivot_time), 'is_overlap_of'] = \
            pivot_exact_overlapped(pivot_time, multi_timeframe_pivots)
        multi_timeframe_pivots = update_hit(pivot_time, pivot_info, multi_timeframe_pivots)
        multi_timeframe_pivots = update_inactive_levels(pivot_time, pivot_info, multi_timeframe_pivots)

    update_levels()
