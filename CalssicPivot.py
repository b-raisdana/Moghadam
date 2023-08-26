from datetime import datetime

import pandas as pd
import pandera
from pandera import typing as pt

from BullBearSidePivot import generate_multi_timeframe_bull_bear_side_pivots
from ColorTrend import generate_multi_timeframe_color_trend_pivots
from DataPreparation import MultiTimeframe


class Pivot(pandera.DataFrameModel):
    date: pt.Index[datetime]
    movement_leg: pt.Series[datetime]
    return_leg: pt.Series[datetime]
    level: pt.Series[float]
    internal_margin: pt.Series[float]
    external_margin: pt.Series[float]
    is_active: pt.Series[bool]
    hit: pt.Series[int]


class MultiTimeframePivot(Pivot, MultiTimeframe):
    pass


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


def generate_multi_timeframe_anti_pattern_tops_pivots():
    # tops of timeframe which the timeframe is its pattern timeframe
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
