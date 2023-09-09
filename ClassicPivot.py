import os

import pandas as pd
from pandera import typing as pt

from BullBearSidePivot import read_multi_timeframe_bull_bear_side_pivots
from PivotsHelper import peaks_or_valleys_pivots_level_n_margins, pivot_margins, pivots_level_n_margins
from Candle import read_multi_timeframe_ohlca
from Model.MultiTimeframeOHLC import OHLCV
from Config import config, TopTYPE
from DataPreparation import single_timeframe, trigger_timeframe, read_file, \
    anti_trigger_timeframe, cast_and_validate, anti_pattern_timeframe
from FigurePlotter.Pivot_plotter import plot_multi_timeframe_pivots
from Model.MultiTimeframePivot import MultiTimeframePivot
from PeakValley import read_multi_timeframe_peaks_n_valleys, peaks_only, valleys_only
from Pivots import pivot_exact_overlapped, level_ttl, update_hit
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


def tops_pivots(date_range_str) -> pt.DataFrame[MultiTimeframePivot]:
    _multi_timeframe_peaks_n_valleys = read_multi_timeframe_peaks_n_valleys(date_range_str)
    _multi_timeframe_ohlca = read_multi_timeframe_ohlca(date_range_str)
    multi_timeframe_pivots = pd.DataFrame()
    for timeframe in config.structure_timeframes[::-1][2:]:
        _pivots = single_timeframe(_multi_timeframe_peaks_n_valleys, anti_trigger_timeframe(timeframe))
        timeframe_ohlca = single_timeframe(_multi_timeframe_ohlca, timeframe)
        trigger_timeframe_ohlca = single_timeframe(_multi_timeframe_ohlca, trigger_timeframe(timeframe))
        _pivots = pivots_level_n_margins(_pivots, _pivots, timeframe, timeframe_ohlca, trigger_timeframe_ohlca)
        _pivots['activation_time'] = _pivots.index
        _pivots['ttl'] = _pivots.index + level_ttl(timeframe)
        _pivots['hit'] = 0  # update_hits(multi_timeframe_pivots)
        _pivots['is_overlap_of'] = None
        _pivots['deactivated_at'] = None
        _pivots['archived_at'] = None

        if len(_pivots) > 0:
            # _pivots = cast_and_validate(_pivots, Pivot)
            _pivots['timeframe'] = anti_pattern_timeframe(timeframe)
            _pivots.set_index('timeframe', append=True, inplace=True)
            _pivots = _pivots.swaplevel()
            multi_timeframe_pivots = pd.concat([multi_timeframe_pivots, _pivots])
    multi_timeframe_pivots = cast_and_validate(multi_timeframe_pivots, MultiTimeframePivot)
    return multi_timeframe_pivots


def read_multi_timeframe_top_pivots(date_range_str: str = config.under_process_date_range):
    result = read_file(date_range_str, 'multi_timeframe_top_pivots',
                       generate_multi_timeframe_top_pivots, MultiTimeframePivot)
    return result


@measure_time
def generate_multi_timeframe_top_pivots(date_range_str: str = config.under_process_date_range,
                                        file_path: str = config.path_of_data):
    # tops of timeframe which the timeframe is its pattern timeframe
    _tops_pivots = tops_pivots(date_range_str)
    plot_multi_timeframe_pivots(_tops_pivots, name='multi_timeframe_top_pivots')
    _tops_pivots.to_csv(
        os.path.join(file_path, f'multi_timeframe_bull_bear_side_pivots.{date_range_str}.zip'),
        compression='zip')


@measure_time
def generate_multi_timeframe_pivot_levels(date_range_str: str = config.under_process_date_range):
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
    multi_timeframe_bull_bear_side_pivots = read_multi_timeframe_bull_bear_side_pivots()
    multi_timeframe_anti_pattern_tops_pivots = read_multi_timeframe_top_pivots()
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
