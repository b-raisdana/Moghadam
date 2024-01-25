import os
from typing import List, Annotated

import pandas as pd
from pandera import typing as pt

from ClassicPivot import insert_ftc, update_pivot_deactivation
from Config import config, TopTYPE
from MetaTrader import MT
from PanderaDFM.AtrTopPivot import MultiTimeframeAtrMovementPivotDFM, MultiTimeframeAtrMovementPivotDf, \
    AtrMovementPivotDf
from PanderaDFM.MultiTimeframe import MultiTimeframe
from PanderaDFM.OHLCV import OHLCV
from PanderaDFM.OHLCVA import MultiTimeframeOHLCVA, OHLCVA
from PanderaDFM.PeakValley import MultiTimeframePeakValley, PeakValley
from PanderaDFM.Pivot import MultiTimeframePivotDFM
from PeakValley import read_multi_timeframe_peaks_n_valleys, insert_top_crossing, peaks_only, valleys_only
from PivotsHelper import pivots_level_n_margins, level_ttl, pivot_margins
from atr import read_multi_timeframe_ohlcva
from helper.data_preparation import single_timeframe, anti_trigger_timeframe, cast_and_validate, \
    read_file, after_under_process_date, empty_df, concat, to_timeframe
from helper.helper import measure_time, date_range, date_range_to_string


def insert_more_significant_top(base_tops: pt.DataFrame[MultiTimeframePeakValley],
                                target_tops: pt.DataFrame[MultiTimeframePeakValley], top_type: TopTYPE) \
        -> pt.DataFrame[MultiTimeframePeakValley]:
    if top_type == TopTYPE.PEAK:
        high_low = 'high'
        cross_direction = 'out'
    else:  # top_type == TopTYPE.VALLEY
        high_low = 'low'
        cross_direction = 'out'

    no_timeframe_tops = target_tops.reset_index(level='timeframe')
    for direction in ['left', 'right']:
        if f'{direction}_crossing_time' not in base_tops.columns:
            base_tops[f'{direction}_crossing_time'] = pd.Series(dtype='datetime64[ns, UTC]')
        if f'{direction}_crossing_value' not in base_tops.columns:
            base_tops[f'{direction}_crossing_value'] = pd.Series(dtype=float)

    peak_or_valley_indexes = base_tops[base_tops['peak_or_valley'] == top_type.value].index
    base_tops.loc[peak_or_valley_indexes, 'target'] = base_tops.loc[peak_or_valley_indexes, high_low]
    base_tops.loc[peak_or_valley_indexes, ['left_crossing_time', 'left_crossing_value']] = \
        insert_top_crossing(base_tops.loc[peak_or_valley_indexes], no_timeframe_tops, top_type, 'left',
                            base_target_column='target',
                            cross_direction=cross_direction)[['left_crossing_time', 'left_crossing_value']]
    base_tops.loc[peak_or_valley_indexes, ['right_crossing_time', 'right_crossing_value']] = \
        insert_top_crossing(base_tops.loc[peak_or_valley_indexes], no_timeframe_tops, top_type, 'right',
                            base_target_column='target',
                            cross_direction=cross_direction)[['right_crossing_time', 'right_crossing_value']]
    return base_tops


@measure_time
def insert_more_significant_tops(timeframe_tops: pt.DataFrame[PeakValley],
                                 compared_tops: pt.DataFrame[MultiTimeframePeakValley]) \
        -> pt.DataFrame[MultiTimeframePeakValley]:
    timeframe_tops.drop(
        columns=['right_crossing_time', 'left_crossing_time', 'right_crossing_value', 'left_crossing_value'],
        inplace=True, errors='ignore')
    insert_more_significant_top(timeframe_tops, compared_tops, TopTYPE.PEAK)  # todo: test
    insert_more_significant_top(timeframe_tops, compared_tops, TopTYPE.VALLEY)
    timeframe_tops.rename(columns={
        'right_crossing_time': 'right_more_significant_top',
        'left_crossing_time': 'left_more_significant_top',
        'right_crossing_value': 'right_more_significant_top_value',
        'left_crossing_value': 'left_more_significant_top_value',
    }, inplace=True)
    return timeframe_tops


@measure_time
def atr_movement_pivots(date_range_str: str = None, structure_timeframe_shortlist: List['str'] = None,
                        same_time_multiple_timeframes: bool = False) -> pt.DataFrame[MultiTimeframeAtrMovementPivotDFM]:
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
    mt_tops: pt.DataFrame[MultiTimeframePeakValley] = read_multi_timeframe_peaks_n_valleys(date_range_str)
    start, end = date_range(date_range_str)
    mt_tops_mapped_time = [to_timeframe(date, timeframe, ignore_cached_times=True, do_not_warn=True)
                           for (timeframe, date), top_data in mt_tops.iterrows()]
    expanded_atr_start = min(mt_tops_mapped_time)
    expanded_atr_date_range_str = date_range_to_string(start=min(start, expanded_atr_start), end=end)
    mt_ohlcva = read_multi_timeframe_ohlcva(expanded_atr_date_range_str)
    base_timeframe_ohlcva = single_timeframe(mt_ohlcva, config.timeframes[0])

    # filter to tops of structure timeframes
    mt_tops = mt_tops[mt_tops.index.get_level_values('timeframe').isin(structure_timeframe_shortlist)]
    all_mt_tops = mt_tops.copy()
    pivots = MultiTimeframeAtrMovementPivotDf.new()
    for timeframe in structure_timeframe_shortlist[::-1]:
        if timeframe in mt_tops.index.get_level_values('timeframe'):
            # timeframe_tops: pt.DataFrame[PeakValley] = major_peaks_n_valleys(mt_tops, timeframe)
            timeframe_tops: pt.DataFrame[PeakValley] = mt_tops.copy()
            timeframe_tops.reset_index(level='timeframe', inplace=True)
            ohlcva: pt.DataFrame[OHLCVA] = single_timeframe(mt_ohlcva, timeframe)
            timeframe_tops = pd.merge_asof(timeframe_tops, ohlcva[['atr']], left_index=True, right_index=True,
                                           direction='backward')
            insert_pivot_movements(timeframe_tops, base_timeframe_ohlcva)

            movement_satisfied_tops = timeframe_tops[
                timeframe_tops['movement_start_time'].notna() & timeframe_tops['return_end_time'].notna()].copy()
            if len(movement_satisfied_tops) > 0:
                insert_more_significant_tops(movement_satisfied_tops, all_mt_tops)

                timeframe_pivots = movement_satisfied_tops[
                    (movement_satisfied_tops['left_more_significant_top'].isna() |
                     (movement_satisfied_tops['left_more_significant_top'] <= movement_satisfied_tops[
                         'movement_start_time']))
                    &
                    (movement_satisfied_tops['right_more_significant_top'].isna() |
                     (movement_satisfied_tops['right_more_significant_top'] >= movement_satisfied_tops[
                         'return_end_time']))
                    ].copy()

                timeframe_pivots['timeframe'] = timeframe
                timeframe_pivots.set_index('timeframe', inplace=True, append=True)
                timeframe_pivots.swaplevel()
                pivots = MultiTimeframeAtrMovementPivotDf.concat(pivots, timeframe_pivots)
                if not ((len(mt_tops.drop(
                        index=mt_tops[mt_tops.index.get_level_values('date') \
                                .isin(timeframe_pivots.index.get_level_values('date'))].index)) + len(timeframe_pivots))
                        == len(mt_tops)):
                    AssertionError("not ((len(mt_tops.drop(...")
                if not same_time_multiple_timeframes:
                    mt_tops = mt_tops.drop(
                        index=mt_tops[mt_tops.index.get_level_values('date') \
                            .isin(timeframe_pivots.index.get_level_values('date'))].index)
    pivots = insert_pivot_info(pivots, mt_ohlcva, structure_timeframe_shortlist, base_timeframe_ohlcva)
    pivots = insert_ftc(pivots, mt_ohlcva)
    return pivots


@measure_time
def insert_pivot_info(pivots, mt_ohlcva, structure_timeframe_shortlist, base_timeframe_ohlcv):
    insert_pivot_level_n_type(pivots)
    if structure_timeframe_shortlist is None:
        structure_timeframe_shortlist = config.structure_timeframes[::-1]
    multi_timeframe_pivots = MultiTimeframeAtrMovementPivotDf.new()
    for timeframe in pivots.index.get_level_values('timeframe').intersection(structure_timeframe_shortlist):
        timeframe_ohlcva = single_timeframe(mt_ohlcva, timeframe)
        timeframe_pivots = pivots[pivots.index.get_level_values(level='timeframe') == timeframe]
        timeframe_pivots = timeframe_pivots.reset_index(level='timeframe')

        timeframe_pivots['activation_time'] = timeframe_pivots.index

        timeframe_resistance_pivots = timeframe_pivots[
            timeframe_pivots['peak_or_valley'] == TopTYPE.PEAK.value].index
        timeframe_pivots.loc[timeframe_resistance_pivots, ['internal_margin', 'external_margin']] = \
            pivot_margins(timeframe_pivots.loc[timeframe_resistance_pivots], _type=TopTYPE.PEAK,
                          pivot_peaks_or_valleys=timeframe_pivots.loc[timeframe_resistance_pivots],
                          candle_body_source=timeframe_ohlcva,
                          # timeframe_pivots.loc[timeframe_resistance_pivots],
                          timeframe=timeframe,
                          internal_margin_atr=timeframe_ohlcva,
                          breakout_margin_atr=timeframe_ohlcva)[['internal_margin', 'external_margin']]

        timeframe_support_pivots = timeframe_pivots[
            timeframe_pivots['peak_or_valley'] == TopTYPE.VALLEY.value].index
        timeframe_pivots.loc[timeframe_support_pivots, ['internal_margin', 'external_margin', ]] = \
            pivot_margins(timeframe_pivots.loc[timeframe_support_pivots], _type=TopTYPE.VALLEY,
                          pivot_peaks_or_valleys=timeframe_pivots.loc[timeframe_support_pivots],
                          candle_body_source=timeframe_ohlcva,
                          # timeframe_pivots.loc[timeframe_support_pivots],
                          timeframe=timeframe,
                          internal_margin_atr=timeframe_ohlcva,
                          breakout_margin_atr=timeframe_ohlcva)[['internal_margin', 'external_margin']]

        timeframe_pivots['ttl'] = timeframe_pivots.index + level_ttl(timeframe)
        timeframe_pivots['deactivated_at'] = pd.Series(dtype='datetime64[ns, UTC]')
        timeframe_pivots['archived_at'] = pd.Series(dtype='datetime64[ns, UTC]')
        timeframe_pivots['master_pivot_timeframe'] = pd.Series(dtype='str')
        timeframe_pivots['master_pivot_date'] = pd.Series(dtype='datetime64[ns, UTC]')
        timeframe_pivots['hit'] = 0
        timeframe_pivots = AtrMovementPivotDf.cast_and_validate(timeframe_pivots)
        timeframe_pivots['timeframe'] = timeframe
        timeframe_pivots = timeframe_pivots.set_index('timeframe', append=True)
        timeframe_pivots = timeframe_pivots.swaplevel()
        multi_timeframe_pivots: pt.DataFrame[MultiTimeframeAtrMovementPivotDFM] = \
            MultiTimeframeAtrMovementPivotDf.concat(multi_timeframe_pivots, timeframe_pivots)
    pivots = MultiTimeframeAtrMovementPivotDf.cast_and_validate(multi_timeframe_pivots)
    update_pivot_deactivation(multi_timeframe_pivots, base_timeframe_ohlcv)  # todo: test
    insert_ftc(multi_timeframe_pivots, structure_timeframe_shortlist)
    return pivots


def insert_pivot_level_n_type(pivots):
    resistance_pivots = pivots[pivots['peak_or_valley'] == TopTYPE.PEAK.value].index
    pivots.loc[resistance_pivots, 'level'] = pivots.loc[resistance_pivots, 'high']
    pivots.loc[resistance_pivots, 'is_resistance'] = True
    support_pivots = pivots[pivots['peak_or_valley'] == TopTYPE.VALLEY.value].index
    pivots.loc[support_pivots, 'level'] = pivots.loc[support_pivots, 'low']
    pivots.loc[support_pivots, 'is_resistance'] = False


@measure_time
def insert_pivot_movements(timeframe_tops: pt.DataFrame[PeakValley],
                           base_ohlcv: pt.DataFrame[OHLCV]) -> pt.DataFrame[PeakValley]:
    """

    :param timeframe_tops:
    :param base_ohlcv:
    :return:
    adds:
    "left_crossing_time" = time of first candle before with 3 * ATR adds
    "right_crossing_time" = time of first candle before with 1 * ATR -
    'right_crossing_value', 'left_crossing_value'
    """
    if 'right_crossing_time' not in timeframe_tops.columns:
        timeframe_tops['right_crossing_time'] = pd.Series(Annotated[pd.DatetimeTZDtype, "ns", "UTC"])
    if 'right_crossing_value' not in timeframe_tops.columns:
        timeframe_tops['right_crossing_value'] = pd.Series(float)
    if 'left_crossing_time' not in timeframe_tops.columns:
        timeframe_tops['left_crossing_time'] = pd.Series(Annotated[pd.DatetimeTZDtype, "ns", "UTC"])
    if 'left_crossing_value' not in timeframe_tops.columns:
        timeframe_tops['left_crossing_value'] = pd.Series(float)

    # timeframe_short_list = timeframe_tops.index.get_level_values('timeframe').unique()
    # for timeframe in timeframe_short_list:
    #     ohlcva = single_timeframe(mt_ohlcva, timeframe)
    #     timeframe_tops = mt_tops[mt_tops.index.get_level_values(level='timeframe') == timeframe]
    #     # single_timeframe(mt_tops, timeframe)
    peaks = peaks_only(timeframe_tops).copy()
    peaks_indexes = peaks.index
    # peaks = peaks.droplevel('timeframe')
    valleys = valleys_only(timeframe_tops).copy()
    valleys_indexes = valleys.index
    # valleys = valleys.droplevel('timeframe')

    timeframe_tops.loc[peaks_indexes, ['right_crossing_time', 'left_crossing_time',
                                       'right_crossing_value', 'left_crossing_value']] = \
        insert_pivot_movement_times(peaks, base_ohlcv, top_type=TopTYPE.PEAK
                                    )[['right_crossing_time', 'left_crossing_time',
                                       'right_crossing_value', 'left_crossing_value']]

    timeframe_tops.loc[valleys_indexes, ['right_crossing_time', 'left_crossing_time',
                                         'right_crossing_value', 'left_crossing_value']] = \
        insert_pivot_movement_times(valleys, base_ohlcv, top_type=TopTYPE.VALLEY
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
    #                                 'right_crossing_value', 'left_crossing_value']]  # toddo: test
    #
    #     mt_tops.loc[valleys_indexes, ['right_crossing_time', 'left_crossing_time',
    #                                   'right_crossing_value', 'left_crossing_value']] = \
    #         insert_pivot_passage(valleys, ohlcva, top_type=TopTYPE.VALLEY
    #                              )[['right_crossing_time', 'left_crossing_time',
    #                                 'right_crossing_value', 'left_crossing_value']]
    timeframe_tops['return_end_time'] = timeframe_tops['right_crossing_time'].astype('datetime64[ns, UTC]')
    timeframe_tops['movement_start_time'] = timeframe_tops['left_crossing_time'].astype('datetime64[ns, UTC]')
    timeframe_tops['return_end_value'] = timeframe_tops['right_crossing_value'].astype(float)
    timeframe_tops['movement_start_value'] = timeframe_tops['left_crossing_value'].astype(float)
    timeframe_tops.drop(columns=['right_crossing_time', 'left_crossing_time',
                                 'right_crossing_value', 'left_crossing_value'], inplace=True)
    return timeframe_tops


# @measure_time
def insert_pivot_movement_times(timeframe_peak_or_valleys: pt.DataFrame[PeakValley], base_ohlcv: pt.DataFrame[OHLCV],
                                top_type: TopTYPE) -> pt.DataFrame[MultiTimeframePeakValley]:
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

        def aggregate(high, n_atr):
            return high - n_atr
    else:  # top_type == TopTYPE.VALLEY
        high_low = 'low'

        def aggregate(low, n_atr):
            return low + n_atr

    peak_or_valley_indexes = timeframe_peak_or_valleys[
        timeframe_peak_or_valleys['peak_or_valley'] == top_type.value].index
    peak_or_valleys = timeframe_peak_or_valleys.loc[peak_or_valley_indexes]

    peak_or_valleys['previous_target'] = \
        aggregate(peak_or_valleys[high_low], 3 * timeframe_peak_or_valleys.loc[peak_or_valley_indexes, 'atr'])
    timeframe_peak_or_valleys.loc[peak_or_valley_indexes, ['left_crossing_time', 'left_crossing_value']] = \
        insert_top_crossing(peak_or_valleys, base_ohlcv, top_type, 'left', 'in',
                            base_target_column='previous_target')[['left_crossing_time', 'left_crossing_value']]

    peak_or_valleys['next_target'] = aggregate(peak_or_valleys[high_low], peak_or_valleys['atr'])
    timeframe_peak_or_valleys.loc[peak_or_valley_indexes, ['right_crossing_time', 'right_crossing_value']] = \
        insert_top_crossing(peak_or_valleys, base_ohlcv, top_type, 'right', 'in',
                            base_target_column='next_target')[['right_crossing_time', 'right_crossing_value']]

    return timeframe_peak_or_valleys


# def insert_peak_n_valley_atr(timeframe_tops: pt.DataFrame[PeakValley],
#                              ohlcva: pt.DataFrame[OHLCVA]) -> pt.DataFrame[PeakValley]:
def insert_multi_timeframe_atr(df: pt.DataFrame[MultiTimeframe],
                               mt_ohlcva: pt.DataFrame[MultiTimeframeOHLCVA]) -> pt.DataFrame[
    MultiTimeframePeakValley]:
    timeframe_shortlist = df.index.get_level_values('timeframe').unique()
    for timeframe in timeframe_shortlist:
        ohlcva = single_timeframe(mt_ohlcva, timeframe)

        timeframe_rows = df[df.index.get_level_values('timeframe') == timeframe]
        timeframe_indexes = timeframe_rows.index
        timeframe_rows = timeframe_rows.reset_index(level='timeframe')

        timeframe_rows['atr'] = \
            pd.merge_asof(timeframe_rows, ohlcva[['atr']], left_index=True, right_index=True,
                          direction='backward', suffixes=('_x', ''))['atr']  # todo: test
        timeframe_rows = timeframe_rows.set_index('timeframe', append=True)
        timeframe_rows = timeframe_rows.swaplevel()
        df.loc[timeframe_indexes, 'atr'] = timeframe_rows['atr']
    if df['atr'].isna().any().any():
        raise AssertionError("df['atr'].isna().any().any()")
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
    #                                       direction='backward', suffixes=('_x', ''))['atr']  # toddo: test
    # if timeframe_tops['atr'].isna().any().any():
    #     AssertionError("mt_tops['atr'].isna().any().any()")
    # return timeframe_tops


def major_times_tops_pivots(date_range_str) -> pt.DataFrame[MultiTimeframePivotDFM]:
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
    multi_timeframe_pivots = empty_df(MultiTimeframePivotDFM)
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
        trigger_timeframe_ohlcva = single_timeframe(_multi_timeframe_ohlcva,
                                                    timeframe)  # trigger_timeframe(timeframe))
        _pivots = pivots_level_n_margins(timeframe_pivots=_pivots, pivot_time_peaks_n_valleys=_pivots,
                                         timeframe=timeframe, candle_body_source=timeframe_ohlcva,
                                         internal_atr_source=timeframe_ohlcva,
                                         breakout_atr_source=trigger_timeframe_ohlcva)
        _pivots['activation_time'] = _pivots.index
        _pivots['ttl'] = _pivots.index + level_ttl(timeframe)
        _pivots['hit'] = 0
        _pivots['master_pivot_timeframe'] = None
        _pivots['master_pivot_date'] = None
        _pivots['deactivated_at'] = None
        _pivots['archived_at'] = None
        if len(_pivots) > 0:
            _pivots['timeframe'] = timeframe
            _pivots = _pivots.set_index('timeframe', append=True)
            _pivots = _pivots.swaplevel()
            multi_timeframe_pivots = concat(multi_timeframe_pivots, _pivots)
    multi_timeframe_pivots = multi_timeframe_pivots.sort_index(level='date')
    multi_timeframe_pivots = cast_and_validate(multi_timeframe_pivots, MultiTimeframePivotDFM,
                                               zero_size_allowed=after_under_process_date(date_range_str))
    return multi_timeframe_pivots


def zz_tops_pivots(date_range_str) -> pt.DataFrame[MultiTimeframePivotDFM]:
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
    multi_timeframe_pivots = empty_df(MultiTimeframePivotDFM)
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
        trigger_timeframe_ohlcva = single_timeframe(_multi_timeframe_ohlcva,
                                                    timeframe)  # trigger_timeframe(timeframe))
        _pivots = pivots_level_n_margins(timeframe_pivots=_pivots, pivot_time_peaks_n_valleys=_pivots,
                                         timeframe=timeframe, candle_body_source=timeframe_ohlcva,
                                         internal_atr_source=timeframe_ohlcva,
                                         breakout_atr_source=trigger_timeframe_ohlcva)
        _pivots['activation_time'] = _pivots.index
        _pivots['ttl'] = _pivots.index + level_ttl(timeframe)
        _pivots['hit'] = 0  # update_hits(multi_timeframe_pivots)
        _pivots['master_pivot_timeframe'] = None
        _pivots['master_pivot_date'] = None
        _pivots['deactivated_at'] = None
        _pivots['archived_at'] = None
        if len(_pivots) > 0:
            _pivots['timeframe'] = timeframe
            _pivots = _pivots.set_index('timeframe', append=True)
            _pivots = _pivots.swaplevel()
            multi_timeframe_pivots = concat(multi_timeframe_pivots, _pivots)
    multi_timeframe_pivots = multi_timeframe_pivots.sort_index(level='date')
    multi_timeframe_pivots = cast_and_validate(multi_timeframe_pivots, MultiTimeframePivotDFM,
                                               zero_size_allowed=after_under_process_date(date_range_str))
    return multi_timeframe_pivots


def read_multi_timeframe_major_times_top_pivots(date_range_str: str = None):
    result = read_file(date_range_str, 'multi_timeframe_major_times_top_pivots',
                       generate_multi_timeframe_major_times_top_pivots, MultiTimeframePivotDFM)
    return result


@measure_time
def generate_multi_timeframe_major_times_top_pivots(date_range_str: str = None, file_path: str = None):
    # tops of anti-trigger timeframe
    if date_range_str is None:
        date_range_str = config.processing_date_range
    if file_path is None:
        file_path = config.path_of_data
    _tops_pivots = major_times_tops_pivots(date_range_str)
    _tops_pivots = _tops_pivots.sort_index(level='date')
    _tops_pivots.to_csv(
        os.path.join(file_path, f'multi_timeframe_top_pivots.{date_range_str}.zip'),
        compression='zip')
    MT.extract_to_data_path(
        os.path.join(file_path, f'multi_timeframe_major_times_top_pivots.{date_range_str}.zip'))
