import os
import pandas as pd
from pandera import typing as pt

from BullBearSide import read_multi_timeframe_bull_bear_side_trends, previous_trend
from CalssicPivot import MultiTimeframePivot
from Candle import read_multi_timeframe_ohlca
from Config import config
from DataPreparation import single_timeframe, tolerance
from PeakValley import read_multi_timeframe_peaks_n_valleys, major_peaks_n_valleys, peaks_only, valleys_only


def multi_timeframe_bull_bear_side_pivots(date_range_str: str = config.under_process_date_range) \
        -> pt.DataFrame[MultiTimeframePivot]:
    """
    highest high of every Bullish and lowest low of every Bearish trend. for Trends
            conditions:
                >= 3 ATR
                >= 1 ATR reverse movement after the most significant top before a trend with the same direction.
                if a trend with the same direction before < 1 ATR return found, merge these together.
            index = time of pivot candle (highest high for Bullish and lowest low for Bearish) mapped to timeframe
            exact_time = exact time of pivot candle
            timeframe = time frame of pivot candle
            value = highest high for Bullish and lowest low for Bearish
            inner_margin = [Bullish: high - ]/[Bearish: low +]
                max(distance from nearest body of pivot and adjacent candles, 1 ATR in pivot timeframe)
            outer_margin =
    warning: if highest high is not the last peak of Bullish and lowest low is not the last Valley raise a warning log:
                timeframe, trend start time (index), time of last top
                time and high of highest high in Bullish and time and low of lowest low in Bearish,
    :return:
    """
    multi_timeframe_trends = read_multi_timeframe_bull_bear_side_trends(date_range_str)
    multi_timeframe_peaks_n_valleys = read_multi_timeframe_peaks_n_valleys(date_range_str)
    multi_timeframe_ohlca = read_multi_timeframe_ohlca(date_range_str)
    multi_timeframe_pivots = pd.DataFrame
    for _, timeframe in config.timeframes[::-1]:
        single_timeframe_peaks_n_valleys = major_peaks_n_valleys(multi_timeframe_peaks_n_valleys)
        single_timeframe_ohlca = single_timeframe(multi_timeframe_ohlca)
        single_timeframe_trends = single_timeframe(multi_timeframe_trends, timeframe)
        single_timeframe_trends['previous_trend'] = previous_trend(single_timeframe_trends)
        single_timeframe_trends.loc[single_timeframe_trends['previous_trend'].notna(), 'previous_trend_movement'] = \
            single_timeframe_trends.loc[single_timeframe_trends['previous_trend'].notna(), 'movement']
        timeframe_trend_pivot_indexes = single_timeframe_trends[
            (single_timeframe_trends['movement'] > tolerance(single_timeframe_trends['ATR']))
            & (single_timeframe_trends['previous_trend_movement'] > tolerance(single_timeframe_trends['ATR']) * 3)
            ].index.get_level_values('date').unique()
        timeframe_trend_pivots = pd.DataFrame(
            index=single_timeframe_trends.loc[timeframe_trend_pivot_indexes, 'start_time_of_movement'])
        timeframe_trend_pivots['movement_leg'] = single_timeframe_trends.loc[
            timeframe_trend_pivot_indexes, 'previous_trend']
        timeframe_trend_pivots['return_leg'] = timeframe_trend_pivot_indexes
        pivot_peaks_and_valleys = single_timeframe_peaks_n_valleys[
            single_timeframe_peaks_n_valleys.index.get_level_values('date').isin(timeframe_trend_pivots.index)]

        pivot_peaks = peaks_only(pivot_peaks_and_valleys)
        pivot_peak_times = pivot_peaks.index.get_level_values('date')
        timeframe_trend_pivots.loc[pivot_peak_times, 'level'] = pivot_peaks_and_valleys['high']
        timeframe_trend_pivots.loc[pivot_peak_times, 'nearest_body'] = \
            pivot_peaks_and_valleys[['open', 'close']].max(axis='columns')
        timeframe_trend_pivots.loc[pivot_peak_times, 'ATR_margin'] = \
            pivot_peaks_and_valleys['high'] - single_timeframe_ohlca.loc[pivot_peak_times, 'ATR']
        timeframe_trend_pivots.loc[pivot_peak_times, 'internal_margin'] = \
            timeframe_trend_pivots.loc[pivot_peak_times, ['nearest_body', 'ATR_margin']].min(axis='columns')

        pivot_valleys = valleys_only(pivot_peaks_and_valleys)
        pivot_valley_times = pivot_valleys.index.get_level_values('date')
        timeframe_trend_pivots.loc[pivot_valley_times, 'level'] = pivot_peaks_and_valleys['low']
        timeframe_trend_pivots.loc[pivot_valley_times, 'nearest_body'] = \
            pivot_peaks_and_valleys[['open', 'close']].min(axis='columns')
        timeframe_trend_pivots.loc[pivot_valley_times, 'ATR_margin'] = \
            pivot_peaks_and_valleys['low'] + single_timeframe_ohlca.loc[pivot_valley_times, 'ATR']
        timeframe_trend_pivots.loc[pivot_valley_times, 'internal_margin'] = \
            timeframe_trend_pivots.loc[pivot_valley_times, ['nearest_body', 'ATR_margin']].max(axis='columns')

        timeframe_trend_pivots['overlapped_with_major_timeframe'] = None
        if len(multi_timeframe_pivots) > 0:
            for pivot_time, pivot in timeframe_trend_pivots.iterrows():
                indexes_of_overlapping_from_major_timeframes = \
                    multi_timeframe_pivots[multi_timeframe_pivots.index.get_level_values('date') == pivot_time]
                most_major_timeframe = multi_timeframe_pivots[
                    multi_timeframe_pivots.index.get_level_values('date').isin(
                        indexes_of_overlapping_from_major_timeframes)
                    & multi_timeframe_pivots['overlapped_with_major_timeframe'].isnull()].index[0]
                timeframe_trend_pivots.loc[pivot_time, 'overlapped_with_major_timeframe'] = most_major_timeframe

        timeframe_trend_pivots['timeframe'] = timeframe
        timeframe_trend_pivots.set_index('timeframe', append=True, inplace=True)
        timeframe_trend_pivots = timeframe_trend_pivots.swaplevel()
        multi_timeframe_pivots = pd.concat([timeframe_trend_pivots, multi_timeframe_ohlca])
    MultiTimeframePivot.validate(multi_timeframe_pivots)
    return multi_timeframe_pivots


def generate_multi_timeframe_bull_bear_side_pivots(date_range_str: str = config.under_process_date_range,
                                                   file_path: str = config.path_of_data):
    """
    highest high of every Bullish and lowest low of every Bearish trend. for Trends
            conditions:
                >= 3 ATR
                >= 1 ATR reverse movement after the most significant top before a trend with the same direction.
                if a trend with the same direction before < 1 ATR return found, merge these together.
            index = time of pivot candle (highest high for Bullish and lowest low for Bearish) mapped to timeframe
            exact_time = exact time of pivot candle
            timeframe = time frame of pivot candle
            value = highest high for Bullish and lowest low for Bearish
            inner_margin = [Bullish: high - ]/[Bearish: low +]
                max(distance from nearest body of pivot and adjacent candles, 1 ATR in pivot timeframe)
            outer_margin =
    warning: if highest high is not the last peak of Bullish and lowest low is not the last Valley raise a warning log:
                timeframe, trend start time (index), time of last top
                time and high of highest high in Bullish and time and low of lowest low in Bearish,
    :return:
    """
    multi_timeframe_pivots = multi_timeframe_bull_bear_side_pivots(date_range_str)
    plot_multi_timeframe_bull_bear_side_pivots(multi_timeframe_pivots)
    multi_timeframe_pivots.to_csv(
        os.path.join(file_path, f'multi_timeframe_bull_bear_side_pivots.{date_range_str}.zip'),
        compression='zip')

    """
        in all boundaries with movement >= 3 ATR:
            if 
                movement of next boundary >= 1 ATR
                or distance of most significant peak to reverse high/low of next boundary >= 1 ATR 
                or in boundary reverse tops after most significant top find distance of >= 1 ATR
            then:
                the boundary most significant top is a static level pivot.
                if:
                    the pivot is inside a previous active or inactive level of the same or higher timeframe:
                then:
                    do not addd as a new level and increase hit count of the previous level.
                else:
                    add a new level for the pivot   
    """
    # todo: complete generate_multi_timeframe_bull_bear_side_trend_pivots
    raise Exception('Not implemented')
