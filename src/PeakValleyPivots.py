import os
from typing import List, Annotated

import pandas as pd
from pandera import typing as pt

from Config import config, TopTYPE
from MetaTrader import MT
from PanderaDFM.MultiTimeframe import MultiTimeframe
from PanderaDFM.OHLCV import OHLCV
from PanderaDFM.OHLCVA import MultiTimeframeOHLCVA, OHLCVA
from PanderaDFM.PeakValley import MultiTimeframePeakValley, PeakValley
from PanderaDFM.Pivot import MultiTimeframePivot
from PeakValley import read_multi_timeframe_peaks_n_valleys, insert_crossing, peaks_only, valleys_only
from PivotsHelper import pivots_level_n_margins, level_ttl
from atr import read_multi_timeframe_ohlcva
from helper.data_preparation import single_timeframe, anti_trigger_timeframe, cast_and_validate, \
    read_file, after_under_process_date, empty_df, concat, to_timeframe
from helper.helper import measure_time, date_range, date_range_to_string


def insert_more_significant_top(base_tops: pt.DataFrame[MultiTimeframePeakValley],
                                target_tops: pt.DataFrame[MultiTimeframeOHLCVA], top_type: TopTYPE) \
        -> pt.DataFrame[MultiTimeframePeakValley]:
    if top_type == TopTYPE.PEAK:
        high_low = 'high'

        def target(high, atr):
            return high + atr
    else:  # top_type == TopTYPE.VALLEY
        high_low = 'low'

        def target(low, atr):
            return low - atr
    # if 'atr' not in base_tops.columns:  # todo: test
    #     base_tops = insert_peak_n_valley_atr(base_tops, target_tops)
    assert 'atr' in base_tops.columns
    indexes = base_tops[base_tops['peak_or_valley'] == top_type.value]

    base_tops.loc[indexes, 'next_target'] = target(base_tops[indexes, high_low], base_tops[indexes, 'atr'])
    base_tops.loc[indexes] = insert_crossing(base_tops.loc[indexes], target_tops, top_type,
                                             'right', base_target_column='next_target')

    base_tops.loc[indexes, 'previous_target'] = target(base_tops[indexes, high_low], 3 * base_tops[indexes, 'atr'])
    base_tops.loc[indexes] = insert_crossing(base_tops.loc[indexes], target_tops, top_type,
                                             'left', base_target_column='previous_target')
    return base_tops


def insert_more_significant_tops(mt_tops: pt.DataFrame[MultiTimeframePeakValley],
                                 mt_ohlcva: pt.DataFrame[MultiTimeframeOHLCVA]) -> pt.DataFrame[
    MultiTimeframePeakValley]:  # todo: test
    mt_tops = insert_more_significant_top(mt_tops, mt_ohlcva, TopTYPE.PEAK)
    mt_tops = insert_more_significant_top(mt_tops, mt_ohlcva, TopTYPE.VALLEY)
    return mt_tops


def atr_top_pivots(date_range_str: str = None, structure_timeframe_shortlist: List['str'] = None) \
        -> pt.DataFrame[MultiTimeframePivot]:
    """
    for peaks:
        (
            (find first candle before far >= 3*ATR:
                if there is not a peak between these 2)
                and
            (find first candle after far >= 1*ATR:
                if there is not a peak between these 2)
        ):
            the peak is a pivot
    :param date_range_str:
    :param structure_timeframe_shortlist:
    :return:
    """
    if structure_timeframe_shortlist is None:
        structure_timeframe_shortlist = config.structure_timeframes
    if date_range_str is None:
        date_range_str = config.processing_date_range
    mt_tops: pt.DataFrame[MultiTimeframePeakValley] = read_multi_timeframe_peaks_n_valleys(date_range_str)  # todo: test
    start, end = date_range(date_range_str)
    mt_tops_mapped_time = [to_timeframe(date, timeframe, ignore_cached_times=True) for (timeframe, date), top_data
                           in mt_tops.iterrows()]
    expanded_atr_start = min(mt_tops_mapped_time)
    expanded_atr_date_range_str = date_range_to_string(start=min(start, expanded_atr_start), end=end)
    mt_ohlcva = read_multi_timeframe_ohlcva(expanded_atr_date_range_str)
    base_ohlcva = single_timeframe(mt_ohlcva, config.timeframes[0])
    # filter to tops of structure timeframes
    mt_tops = mt_tops[mt_tops.index.get_level_values('timeframe').isin(structure_timeframe_shortlist)]
    mt_tops = insert_multi_timeframe_atr(mt_tops, mt_ohlcva)
    mt_tops = insert_pivot_passages(mt_tops, base_ohlcva)
    mt_tops['right_crossing_candle_time'] = mt_tops['right_crossing_time']
    mt_tops['left_crossing_candle_time'] = mt_tops['left_crossing_time']
    passages_satisfied_tops = mt_tops[mt_tops['right_crossing'].notna() and mt_tops['left_crossing'].notna()].copy()
    mt_tops = mt_tops[passages_satisfied_tops]  # Optional
    mt_tops = insert_more_significant_tops(mt_tops, mt_ohlcva)
    pivots = mt_tops[
        (mt_tops['right_more_significant_peak_time'] >= mt_tops['right_crossing_candle_time'])
        and (mt_tops['left_more_significant_peak_time'] <= mt_tops['left_crossing_candle_time'])
        ]
    return pivots
    # """
    # for timeframe:
    #     peaks/valleys with any :
    #         -valley/peak after it's previous peaks/valleys which ist's high/low is
    #             3 ATR gt/lt peaks/valleys high/low and
    # """
    # # multi_timeframe_trends = read_multi_timeframe_bull_bear_side_trends(date_range_str)  # todo: test
    #
    # # add previous and next peaks and valleys
    # peaks = peaks_only(mt_tops)
    # valleys = valleys_only(mt_tops)
    # mt_tops = peak_or_valley_add_adjacent_tops(mt_tops, peaks, valleys, top_type)
    # # mt_tops['previous_peak_value'] = mt_tops.loc[mt_tops['previous_peak_time'].values(), 'high']
    # # merge valleys into tops to extract adjacent valley
    # mt_tops = pd.merge_asof(mt_tops, valleys[['adjacent_top']], left_index=True, right_on='forward_index',
    #                         direction='forward',
    #                         suffixes=("_x", ''))
    # mt_tops['next_valley_time'] = mt_tops['adjacent_top']
    # # mt_tops['next_valley_value'] = mt_tops.loc[mt_tops['next_valley_time'].values(), 'low']
    # mt_tops = pd.merge_asof(mt_tops, valleys[['adjacent_top']], left_index=True, right_on='backward_index',
    #                         direction='backward',
    #                         suffixes=("_x", ''))
    # mt_tops['previous_valley_time'] = mt_tops['adjacent_top']
    # # mt_tops['previous_valley_value'] = mt_tops.loc[mt_tops['previous_valley_time'].values(), 'low']
    # long_trends = multi_timeframe_trends[multi_timeframe_trends['movement'] >= multi_timeframe_trends['atr']]
    # # find peaks surrounded with enough far valleys:
    #
    # """
    #     movement_start_value: pt.Series[float] = pandera.Field(nullable=True)
    #     movement_end_value: pt.Series[float] = pandera.Field(nullable=True)
    #     movement_start_time: pt.Series[Annotated[pd.DatetimeTZDtype, "ns", "UTC"]] = pandera.Field(nullable=True)
    #     movement_end_time
    #     """
    # """
    #     if the boundary movement > 3 ATR:
    #         pivot_return:
    #             there is no trend ends between this trend's end and first candle after movement_end_time:
    #                 Bullish: low < movement_end_value - 1 ATR
    #                 Bearish: high > movement_end_value + 1 ATR
    # :param date_range_str:
    # :param structure_timeframe_shortlist:
    # :return:
    # """
    # # if there is a trand which ends after the trend, if there is a >= 1 ATR jump between their end, we have a pivot.
    # for index, trend in long_trends.iterrows():
    # # return_time = first_return_confirmation_candle()
    #
    # raise NotImplementedError


@measure_time
def insert_pivot_passages(mt_tops: pt.DataFrame[MultiTimeframePeakValley],
                          base_ohlcv: pt.DataFrame[OHLCV]) -> pt.DataFrame[MultiTimeframePeakValley]:
    """

    :param mt_tops:
    :param base_ohlcv:
    :return:
    adds:
    "left_crossing_time" = time of first candle before with 3 * ATR adds
    "right_crossing_time" = time of first candle before with 1 * ATR -
    'right_crossing_value', 'left_crossing_value'
    """
    if 'right_crossing_time' not in mt_tops.columns:
        mt_tops['right_crossing_time'] = pd.Series(Annotated[pd.DatetimeTZDtype, "ns", "UTC"])
    if 'right_crossing_value' not in mt_tops.columns:
        mt_tops['right_crossing_value'] = pd.Series(float)
    if 'left_crossing_time' not in mt_tops.columns:
        mt_tops['left_crossing_time'] = pd.Series(Annotated[pd.DatetimeTZDtype, "ns", "UTC"])
    if 'left_crossing_value' not in mt_tops.columns:
        mt_tops['left_crossing_value'] = pd.Series(float)

    timeframe_short_list = mt_tops.index.get_level_values('timeframe').unique()
    # for timeframe in timeframe_short_list:
    #     ohlcva = single_timeframe(mt_ohlcva, timeframe)
    #     timeframe_tops = mt_tops[mt_tops.index.get_level_values(level='timeframe') == timeframe]
    #     # single_timeframe(mt_tops, timeframe)
    peaks = peaks_only(mt_tops)
    peaks_indexes = peaks.index
    # peaks = peaks.droplevel('timeframe')
    valleys = valleys_only(mt_tops)
    valleys_indexes = valleys.index
    # valleys = valleys.droplevel('timeframe')

    mt_tops.loc[peaks_indexes, ['right_crossing_time', 'left_crossing_time',
                                'right_crossing_value', 'left_crossing_value']] = \
        insert_pivot_passage(peaks, base_ohlcv, top_type=TopTYPE.PEAK
                             )[['right_crossing_time', 'left_crossing_time',
                                'right_crossing_value', 'left_crossing_value']]

    mt_tops.loc[valleys_indexes, ['right_crossing_time', 'left_crossing_time',
                                  'right_crossing_value', 'left_crossing_value']] = \
        insert_pivot_passage(valleys, base_ohlcv, top_type=TopTYPE.VALLEY
                             )[['right_crossing_time', 'left_crossing_time',
                                'right_crossing_value', 'left_crossing_value']]
    # timeframe_short_list = mt_tops.index.get_level_values('timeframe').unique()
    # for timeframe in timeframe_short_list:
    #     ohlcva = single_timeframe(mt_ohlcva, timeframe)
    #     timeframe_tops = mt_tops[mt_tops.index.get_level_values(level='timeframe') == timeframe]
    #     # single_timeframe(mt_tops, timeframe)
    #     peaks = peaks_only(timeframe_tops)
    #     peaks_indexes = peaks.index
    #     peaks = peaks.droplevel('timeframe')
    #     valleys = valleys_only(timeframe_tops)
    #     valleys_indexes = valleys.index
    #     valleys = valleys.droplevel('timeframe')
    #
    #     mt_tops.loc[peaks_indexes, ['right_crossing_time', 'left_crossing_time',
    #                                 'right_crossing_value', 'left_crossing_value']] = \
    #         insert_pivot_passage(peaks, ohlcva, top_type=TopTYPE.PEAK
    #                              )[['right_crossing_time', 'left_crossing_time',
    #                                 'right_crossing_value', 'left_crossing_value']]  # todo: test
    #
    #     mt_tops.loc[valleys_indexes, ['right_crossing_time', 'left_crossing_time',
    #                                   'right_crossing_value', 'left_crossing_value']] = \
    #         insert_pivot_passage(valleys, ohlcva, top_type=TopTYPE.VALLEY
    #                              )[['right_crossing_time', 'left_crossing_time',
    #                                 'right_crossing_value', 'left_crossing_value']]
    return mt_tops


@measure_time
def insert_pivot_passage(timeframe_peak_or_valleys: pt.DataFrame[MultiTimeframePeakValley],
                         base_ohlcv: pt.DataFrame[OHLCV], top_type: TopTYPE) \
        -> pt.DataFrame[MultiTimeframePeakValley]:
    """
    find the first candle before with 3 * ATR distance and first candle next with 1 * ATR distance.
    :param base_ohlcv:
    :param timeframe_peak_or_valleys:
    :param top_type:
    :return:
    - right_crossing_time and left_crossing_time: Time index where the crossing occurs in the specified direction.
    - right_crossing_value and left_crossing_value: Value of the OHLCV data at the crossing point in the specified direction.
    """

    if top_type == TopTYPE.PEAK:
        high_low = 'high'
        cross_direction = 'down'

        def aggregate(high, n_atr):
            return high - n_atr
    else:  # top_type == TopTYPE.VALLEY
        high_low = 'low'
        cross_direction = 'up'

        def aggregate(low, n_atr):
            return low + n_atr

    peak_or_valley_indexes = timeframe_peak_or_valleys[
        timeframe_peak_or_valleys['peak_or_valley'] == top_type.value].index
    timeframe_peak_or_valleys.loc[peak_or_valley_indexes, 'next_target'] = \
        aggregate(timeframe_peak_or_valleys.loc[peak_or_valley_indexes, high_low],
                  timeframe_peak_or_valleys.loc[peak_or_valley_indexes, 'atr'])
    timeframe_peak_or_valleys.loc[peak_or_valley_indexes, ['right_crossing_time', 'right_crossing_value']] = \
        insert_crossing(timeframe_peak_or_valleys.loc[peak_or_valley_indexes], base_ohlcv, top_type, 'right',
                        cross_direction,
                        base_target_column='next_target')[['right_crossing_time', 'right_crossing_value']]

    timeframe_peak_or_valleys.loc[peak_or_valley_indexes, 'previous_target'] = \
        aggregate(timeframe_peak_or_valleys.loc[peak_or_valley_indexes, high_low], 3 * timeframe_peak_or_valleys.loc[
            peak_or_valley_indexes, 'atr'])
    timeframe_peak_or_valleys.loc[peak_or_valley_indexes, ['left_crossing_time', 'left_crossing_value']] = \
        insert_crossing(timeframe_peak_or_valleys.loc[peak_or_valley_indexes], base_ohlcv, top_type, 'left',
                        cross_direction,
                        base_target_column='previous_target')[['left_crossing_time', 'left_crossing_value']]

    return timeframe_peak_or_valleys


# def insert_peak_n_valley_atr(timeframe_tops: pt.DataFrame[PeakValley],
#                              ohlcva: pt.DataFrame[OHLCVA]) -> pt.DataFrame[PeakValley]:
def insert_multi_timeframe_atr(df: pt.DataFrame[MultiTimeframe],
                               mt_ohlcva: pt.DataFrame[MultiTimeframeOHLCVA]) -> pt.DataFrame[MultiTimeframePeakValley]:
    timeframe_shortlist = df.index.get_level_values('timeframe').unique()
    for timeframe in timeframe_shortlist:
        ohlcva = single_timeframe(mt_ohlcva, timeframe)

        timeframe_rows = df[df.index.get_level_values('timeframe') == timeframe]
        timeframe_indexes = timeframe_rows.index
        timeframe_rows = timeframe_rows.reset_index(level='timeframe')

        timeframe_rows['atr'] = pd.merge_asof(timeframe_rows, ohlcva[['atr']], left_index=True, right_index=True,
                                              direction='backward', suffixes=('_x', ''))['atr']  # todo: test
        timeframe_rows = timeframe_rows.set_index('timeframe', append=True)
        timeframe_rows = timeframe_rows.swaplevel()
        df.loc[timeframe_indexes, 'atr'] = timeframe_rows['atr']
    if df['atr'].isna().any().any():
        AssertionError("df['atr'].isna().any().any()")
    return df
    # # the 'date' index of top based on base_timeframe. the 'date' of mt_ohlcva is according to timeframe.
    # # So we use pd.merge_asof(...) to adopt
    # # timeframe_tops['timeframe_column'] = timeframe_tops.index.get_level_values('timeframe')
    # # timeframe_tops['date_column'] = timeframe_tops.index.get_level_values('date')
    # # timeframe_short_list = timeframe_tops.index.get_level_values('timeframe').unique()
    # # for timeframe in timeframe_short_list:
    # #     timeframe_top_indexes = timeframe_tops[timeframe_tops.index.get_level_values(level='timeframe') == timeframe].index
    # #     ohlcva = single_timeframe(mt_ohlcva, timeframe)
    # timeframe_tops['atr'] = pd.merge_asof(timeframe_tops, ohlcva[['atr']], left_index=True, right_index=True,
    #                                       direction='backward', suffixes=('_x', ''))['atr']  # todo: test
    # if timeframe_tops['atr'].isna().any().any():
    #     AssertionError("mt_tops['atr'].isna().any().any()")
    # return timeframe_tops


def tops_pivots(date_range_str) -> pt.DataFrame[MultiTimeframePivot]:
    """

    :param date_range_str:
    :return:
    """
    '''
    A top (Peak or Valley) have significant impact of price movement in the Trigger Timeframe. for example, 1D Peaks and
    Valleys are not forcing 1D price chart but they impact 1H chart and price in 1H chart react to 1D top levels hit.
    As a result:
        1. 1W tops are creating classic levels for 4H, 1D for 1H and 4H for 15min structure Timeframes.
        2. we use the 1H chart for 1D tops because they are creating 1H classic levels.
    '''
    _multi_timeframe_peaks_n_valleys = read_multi_timeframe_peaks_n_valleys(date_range_str)
    _multi_timeframe_ohlcva = read_multi_timeframe_ohlcva(date_range_str)
    multi_timeframe_pivots = empty_df(MultiTimeframePivot)
    for timeframe in config.structure_timeframes[::-1][2:]:
        # 1W tops are creating classic levels for 4H, 1D for 1H and 4H for 15min structure Timeframes.
        _pivots = single_timeframe(_multi_timeframe_peaks_n_valleys, anti_trigger_timeframe(timeframe))
        ohlcv_start = _multi_timeframe_ohlcva.index.get_level_values('date').min()
        '''
        first part of the chart with the length of anti_trigger_timeframe(timeframe) is not reliable. We have to now 
        about anti_trigger_timeframe(timeframe) to make sure the detected Top is not for a anti-trigger Timeframe.  
        '''
        _pivots = _pivots.loc[ohlcv_start + pd.to_timedelta(anti_trigger_timeframe(timeframe)):]
        # we use the 1H chart for 1D tops because they are creating 1H classic levels.
        timeframe_ohlcva = single_timeframe(_multi_timeframe_ohlcva, timeframe)
        trigger_timeframe_ohlcva = single_timeframe(_multi_timeframe_ohlcva, timeframe)  # trigger_timeframe(timeframe))
        _pivots = pivots_level_n_margins(pivot_peaks_n_valleys=_pivots, timeframe_pivots=_pivots,
                                         timeframe=timeframe,
                                         candle_body_source=timeframe_ohlcva,
                                         internal_atr_source=timeframe_ohlcva,
                                         breakout_atr_source=trigger_timeframe_ohlcva,
                                         )
        _pivots['activation_time'] = _pivots.index
        _pivots['ttl'] = _pivots.index + level_ttl(timeframe)
        _pivots['hit'] = 0
        _pivots['is_overlap_of'] = None
        _pivots['deactivated_at'] = None
        _pivots['archived_at'] = None
        if len(_pivots) > 0:
            _pivots['timeframe'] = timeframe
            _pivots = _pivots.set_index('timeframe', append=True)
            _pivots = _pivots.swaplevel()
            multi_timeframe_pivots = concat(multi_timeframe_pivots, _pivots)
    multi_timeframe_pivots = multi_timeframe_pivots.sort_index(level='date')
    multi_timeframe_pivots = cast_and_validate(multi_timeframe_pivots, MultiTimeframePivot,
                                               zero_size_allowed=after_under_process_date(date_range_str))
    return multi_timeframe_pivots


def zz_tops_pivots(date_range_str) -> pt.DataFrame[MultiTimeframePivot]:
    """

    :param date_range_str:
    :return:
    """
    '''
    A top (Peak or Valley) have significant impact of price movement in the Trigger Timeframe. for example, 1D Peaks and
    Valleys are not forcing 1D price chart but they impact 1H chart and price in 1H chart react to 1D top levels hit.
    As a result:
        1. 1W tops are creating classic levels for 4H, 1D for 1H and 4H for 15min structure Timeframes.
        2. we use the 1H chart for 1D tops because they are creating 1H classic levels.
    '''
    _multi_timeframe_peaks_n_valleys = read_multi_timeframe_peaks_n_valleys(date_range_str)
    _multi_timeframe_ohlcva = read_multi_timeframe_ohlcva(date_range_str)
    multi_timeframe_pivots = empty_df(MultiTimeframePivot)
    for timeframe in config.structure_timeframes[::-1][2:]:
        # 1W tops are creating classic levels for 4H, 1D for 1H and 4H for 15min structure Timeframes.
        _pivots = single_timeframe(_multi_timeframe_peaks_n_valleys, anti_trigger_timeframe(timeframe))
        ohlcv_start = _multi_timeframe_ohlcva.index.get_level_values('date').min()
        '''
        first part of the chart with the length of anti_trigger_timeframe(timeframe) is not reliable. We have to now 
        about anti_trigger_timeframe(timeframe) to make sure the detected Top is not for a anti-trigger Timeframe.  
        '''
        _pivots = _pivots.loc[ohlcv_start + pd.to_timedelta(anti_trigger_timeframe(timeframe)):]
        # we use the 1H chart for 1D tops because they are creating 1H classic levels.
        timeframe_ohlcva = single_timeframe(_multi_timeframe_ohlcva, timeframe)
        trigger_timeframe_ohlcva = single_timeframe(_multi_timeframe_ohlcva, timeframe)  # trigger_timeframe(timeframe))
        _pivots = pivots_level_n_margins(pivot_peaks_n_valleys=_pivots, timeframe_pivots=_pivots,
                                         # timeframe=anti_trigger_timeframe(timeframe),
                                         timeframe=timeframe,
                                         candle_body_source=timeframe_ohlcva,
                                         internal_atr_source=timeframe_ohlcva,
                                         breakout_atr_source=trigger_timeframe_ohlcva,
                                         )
        _pivots['activation_time'] = _pivots.index
        _pivots['ttl'] = _pivots.index + level_ttl(timeframe)
        _pivots['hit'] = 0  # update_hits(multi_timeframe_pivots)
        _pivots['is_overlap_of'] = None
        _pivots['deactivated_at'] = None
        _pivots['archived_at'] = None
        if len(_pivots) > 0:
            _pivots['timeframe'] = timeframe
            _pivots = _pivots.set_index('timeframe', append=True)
            _pivots = _pivots.swaplevel()
            multi_timeframe_pivots = concat(multi_timeframe_pivots, _pivots)
    multi_timeframe_pivots = multi_timeframe_pivots.sort_index(level='date')
    multi_timeframe_pivots = cast_and_validate(multi_timeframe_pivots, MultiTimeframePivot,
                                               zero_size_allowed=after_under_process_date(date_range_str))
    return multi_timeframe_pivots


def read_multi_timeframe_top_pivots(date_range_str: str = None):
    result = read_file(date_range_str, 'multi_timeframe_top_pivots',
                       generate_multi_timeframe_top_pivots, MultiTimeframePivot)
    return result


@measure_time
def generate_multi_timeframe_top_pivots(date_range_str: str = None, file_path: str = None):
    # tops of anti-trigger timeframe
    if date_range_str is None:
        date_range_str = config.processing_date_range
    if file_path is None:
        file_path = config.path_of_data
    _tops_pivots = tops_pivots(date_range_str)
    _tops_pivots = _tops_pivots.sort_index(level='date')
    _tops_pivots.to_csv(
        os.path.join(file_path, f'multi_timeframe_top_pivots.{date_range_str}.zip'),
        compression='zip')
    MT.extract_to_data_path(os.path.join(file_path, f'multi_timeframe_top_pivots.{date_range_str}.zip'))
