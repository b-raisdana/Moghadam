import os
from typing import List

import pandas as pd
from pandera import typing as pt

from Config import config, TopTYPE
from MetaTrader import MT
from PanderaDFM.OHLCVA import MultiTimeframeOHLCVA, OHLCVA
from PanderaDFM.PeakValley import MultiTimeframePeakValley, PeakValley
from PanderaDFM.Pivot import MultiTimeframePivot
from PeakValley import read_multi_timeframe_peaks_n_valleys, insert_crossing
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
    if 'atr' not in base_tops.columns:  # todo: test
        base_tops = insert_peak_n_valley_atr(base_tops, target_tops)
    indexes = base_tops[base_tops['peak_or_valley'] == top_type.value]

    base_tops.loc[indexes, 'next_target'] = target(base_tops[indexes, high_low], base_tops[indexes, 'atr'])
    base_tops.loc[indexes] = insert_crossing(base_tops.loc[indexes], target_tops, top_type,
                                             'right', base_compare_column='next_target')

    base_tops.loc[indexes, 'previous_target'] = target(base_tops[indexes, high_low], 3 * base_tops[indexes, 'atr'])
    base_tops.loc[indexes] = insert_crossing(base_tops.loc[indexes], target_tops, top_type,
                                             'left', base_compare_column='previous_target')
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
    mt_ohlcva: pt.DataFrame[MultiTimeframeOHLCVA] = read_multi_timeframe_ohlcva(expanded_atr_date_range_str)
    mt_tops = mt_tops[mt_tops.index.get_level_values('timeframe').isin(structure_timeframe_shortlist)]
    mt_tops = insert_pivot_passages(mt_tops, mt_ohlcva, )
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


def insert_pivot_passages(mt_tops: pt.DataFrame[MultiTimeframePeakValley],
                          mt_ohlcva: pt.DataFrame[MultiTimeframeOHLCVA]) -> pt.DataFrame[MultiTimeframePeakValley]:
    timeframe_short_list = mt_tops.index.get_level_values('timeframe').unique()
    for timeframe in timeframe_short_list:
        ohlcva = single_timeframe(mt_ohlcva, timeframe)
        timeframe_tops = single_timeframe(mt_tops, timeframe)
        timeframe_top_indexes = timeframe_tops.index
        """
        "left_crossing_time" = time of first candle before with 3 * ATR adds 
        "right_crossing_time" = time of first candle before with 1 * ATR - 
        right_crossing or left_crossing: Boolean indicating whether OHLCV data is crossing the peak/valley. - 
        right_crossing_time or left_crossing_time: Time index where the crossing occurs in the specified direction. - right_crossing_value or left_crossing_value: Value of the OHLCV data at the crossing point in the specified direction.
        """
        mt_tops.loc[timeframe_top_indexes, ['right_crossing', 'left_crossing', 'right_crossing_time',
                                            'left_crossing_time', 'right_crossing_value', 'left_crossing_value']] = \
            insert_pivot_passage(timeframe_tops, ohlcva, top_type=TopTYPE.PEAK
                                 )[['right_crossing', 'left_crossing', 'right_crossing_time',
                                    'left_crossing_time', 'right_crossing_value', 'left_crossing_value']]  # todo: test
        mt_tops.loc[timeframe_top_indexes, ['right_crossing', 'left_crossing', 'right_crossing_time',
                                            'left_crossing_time', 'right_crossing_value', 'left_crossing_value']] = \
            insert_pivot_passage(timeframe_tops, ohlcva, top_type=TopTYPE.VALLEY
                                 )[['right_crossing', 'left_crossing', 'right_crossing_time',
                                    'left_crossing_time', 'right_crossing_value', 'left_crossing_value']]
    return mt_tops


def insert_pivot_passage(timeframe_tops: pt.DataFrame[MultiTimeframePeakValley],
                         ohlcva: pt.DataFrame[OHLCVA], top_type: TopTYPE) \
        -> pt.DataFrame[MultiTimeframePeakValley]:
    """
    find the first candle before with 3 * ATR distance and first candle next with 1 * ATR distance.
    :param ohlcva:
    :param timeframe_tops:
    :param top_type:
    :return:
    adds "left_crossing_time" = time of first candle before with 3 * ATR
    adds "right_crossing_time" = time of first candle before with 1 * ATR
    - right_crossing and left_crossing: Boolean indicating whether OHLCV data is crossing the peak/valley.
    - right_crossing_time and left_crossing_time: Time index where the crossing occurs in the specified direction.
    - right_crossing_value and left_crossing_value: Value of the OHLCV data at the crossing point in the specified direction.

    adds "atr" = ATR at the time
    """
    if top_type == TopTYPE.PEAK:
        high_low = 'high'
        cross_direction = 'down'
    else:  # top_type == TopTYPE.VALLEY
        high_low = 'low'
        cross_direction = 'up'
    if 'atr' not in timeframe_tops.columns:  # todo: test
        timeframe_tops = insert_peak_n_valley_atr(timeframe_tops, ohlcva)
    peak_or_valley_indexes = timeframe_tops[timeframe_tops['peak_or_valley'] == top_type.value].index
    timeframe_tops.loc[peak_or_valley_indexes, 'next_target'] = \
        timeframe_tops.loc[peak_or_valley_indexes, high_low] - timeframe_tops.loc[peak_or_valley_indexes, 'atr']
    timeframe_tops.loc[peak_or_valley_indexes, ['right_crossing_time', 'right_crossing_value']] = \
        insert_crossing(timeframe_tops.loc[peak_or_valley_indexes], ohlcva, top_type,
                        'right', cross_direction, base_compare_column='next_target')[['right_crossing_time', 'right_crossing_value']]
    timeframe_tops.loc[peak_or_valley_indexes, 'previous_target'] = \
        timeframe_tops.loc[peak_or_valley_indexes, high_low] - 3 * timeframe_tops.loc[peak_or_valley_indexes, 'atr']
    timeframe_tops.loc[peak_or_valley_indexes, ['left_crossing_time', 'left_crossing_value']] = \
        insert_crossing(timeframe_tops.loc[peak_or_valley_indexes], ohlcva, top_type,
                        'left', cross_direction, base_compare_column='previous_target')[['left_crossing_time', 'left_crossing_value']]
    return timeframe_tops


def insert_peak_n_valley_atr(timeframe_tops: pt.DataFrame[PeakValley],
                             ohlcva: pt.DataFrame[OHLCVA]) -> pt.DataFrame[PeakValley]:
    # the 'date' index of top based on base_timeframe. the 'date' of mt_ohlcva is according to timeframe.
    # So we use pd.merge_asof(...) to adopt
    # timeframe_tops['timeframe_column'] = timeframe_tops.index.get_level_values('timeframe')
    # timeframe_tops['date_column'] = timeframe_tops.index.get_level_values('date')
    # timeframe_short_list = timeframe_tops.index.get_level_values('timeframe').unique()
    # for timeframe in timeframe_short_list:
    #     timeframe_top_indexes = timeframe_tops[timeframe_tops.index.get_level_values(level='timeframe') == timeframe].index
    #     ohlcva = single_timeframe(mt_ohlcva, timeframe)
    timeframe_tops['atr'] = pd.merge_asof(timeframe_tops, ohlcva[['atr']], left_index=True, right_index=True,
                                          direction='backward', suffixes=('_x', ''))['atr'] # todo: test
    if timeframe_tops['atr'].isna().any().any():
        AssertionError("mt_tops['atr'].isna().any().any()")
    return timeframe_tops


def peak_or_valley_add_adjacent_tops(mt_tops, tops, top_type: TopTYPE):
    tops['forward_index'] = tops.index.shift(-1, freq=config.timeframe[0])
    tops['backward_index'] = tops.index.shift(1, freq=config.timeframe[0])
    tops['adjacent_top'] = tops.index
    # merge peaks into tops to extract adjacent peaks
    mt_tops = pd.merge_asof(mt_tops, tops[['adjacent_top']], left_index=True, right_on='forward_index',
                            direction='forward',
                            suffixes=("_x", ''))
    mt_tops[f'next_{top_type.value}_time'] = mt_tops['adjacent_top']
    # mt_tops['next_peak_value'] = mt_tops.loc[mt_tops['next_peak_time'].values(), 'high']
    mt_tops = pd.merge_asof(mt_tops, tops[['adjacent_top']], left_index=True, right_on='backward_index',
                            direction='backward',
                            suffixes=("_x", ''))
    mt_tops['previous_{top_type.value}_time'] = mt_tops['adjacent_top']
    return mt_tops


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
