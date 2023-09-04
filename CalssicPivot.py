import datetime

import pandas as pd
from pandera import typing as pt

from BullBearSidePivot import generate_multi_timeframe_bull_bear_side_pivots, pivot_margins
from Candle import read_multi_timeframe_ohlca, OHLC
from ColorTrend import generate_multi_timeframe_color_trend_pivots
from Config import config, TopTYPE
from DataPreparation import single_timeframe, trigger_timeframe, anti_pattern_timeframe
from Model.MultiTimeframePivot import MultiTimeframePivot
from PeakValley import read_multi_timeframe_peaks_n_valleys


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
    multi_timeframe_pivots = update_inactive_levels(multi_timeframe_pivots)
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





def reactivated_passed_levels(_time, ohlc: pt.DataFrame[OHLC], multi_timeframe_pivots: pt.DataFrame[MultiTimeframePivot]) \
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


def update_inactive_levels(update__time, ohlc: pt.DataFrame[OHLC],
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
    # todo: test update_levels
    for time in range(start_time + pd.to_datetime(timeframe), end_time, pd.to_datetime(timeframe)):

    update_active_levels()
    update_inactive_levels()


def pivot_exact_overlapped(pivot_time, multi_timeframe_pivots):
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
    raise Exception(f'Expected to find a root_pivot but found zero')


def level_ttl(timeframe) -> datetime.timedelta:
    return 256 * pd.to_timedelta(timeframe)


def anti_pattern_tops_pivots(date_range_str) -> pt.DataFrame[MultiTimeframePivot]:
    _multi_timeframe_peaks_n_valleys = read_multi_timeframe_peaks_n_valleys(date_range_str)
    _multi_timeframe_ohlca = read_multi_timeframe_ohlca(date_range_str)
    multi_timeframe_pivots = pd.DataFrame()
    for timeframe in config.structure_timeframes[::-1][1:]:
        _pivots = single_timeframe(_multi_timeframe_peaks_n_valleys, anti_pattern_timeframe(timeframe))
        timeframe_ohlca = single_timeframe(_multi_timeframe_ohlca, timeframe)
        trigger_timeframe_ohlca = single_timeframe(_multi_timeframe_ohlca, trigger_timeframe(timeframe))

        _pivots.loc[_pivots['peak_or_valley'] == TopTYPE.PEAK.value, 'level'] = _pivots['high']
        _pivots.loc[_pivots['peak_or_valley'] == TopTYPE.VALLEY.value, 'level'] = _pivots['low']
        _pivots[_pivots['peak_or_valley'] == TopTYPE.PEAK.value, ['internal_margin', 'external_margin']] = \
            pivot_margins(_pivots, TopTYPE.PEAK, _pivots,
                          timeframe_ohlca, timeframe, trigger_timeframe_ohlca)
        _pivots[_pivots['peak_or_valley'] == TopTYPE.VALLEY.value, ['internal_margin', 'external_margin']] = \
            pivot_margins(_pivots, TopTYPE.VALLEY, _pivots,
                          timeframe_ohlca, timeframe, trigger_timeframe_ohlca)
        _pivots['activation_time'] = _pivots.index
        _pivots['ttl'] = _pivots.index + level_ttl(timeframe)
        _pivots['hit'] = 0  # update_hits(multi_timeframe_pivots)
        _pivots['is_overlap_of'] = [
            pivot_exact_overlapped(_pivots.index[i], multi_timeframe_pivots)
            for i in range(_pivots)
        ]

        if len(_pivots) > 0:
            _pivots = MultiTimeframePivot.validate(_pivots)
            multi_timeframe_pivots = pd.concat([multi_timeframe_pivots, _pivots])
    return multi_timeframe_pivots


def generate_multi_timeframe_anti_pattern_tops_pivots(date_range_str: str = config.under_process_date_range):
    # tops of timeframe which the timeframe is its pattern timeframe
    _anti_pattern_tops_pivots = anti_pattern_tops_pivots(date_range_str)

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
