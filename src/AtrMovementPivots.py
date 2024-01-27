from typing import List, Annotated

import pandas as pd
from pandera import typing as pt

from ClassicPivot import insert_pivot_info
from Config import config, TopTYPE
from PanderaDFM.AtrTopPivot import MultiTimeframeAtrMovementPivotDFM, MultiTimeframeAtrMovementPivotDf
from PanderaDFM.OHLCV import OHLCV
from PanderaDFM.OHLCVA import OHLCVA
from PanderaDFM.PeakValley import MultiTimeframePeakValley, PeakValley
from PeakValley import read_multi_timeframe_peaks_n_valleys, peaks_only, valleys_only, insert_top_crossing
from atr import read_multi_timeframe_ohlcva
from helper.data_preparation import to_timeframe, single_timeframe, pattern_timeframe
from helper.helper import measure_time, date_range, date_range_to_string


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
    insert_major_timeframe(pivots, structure_timeframe_shortlist)
    for timeframe in structure_timeframe_shortlist[::-1]:
        if timeframe in pivots.index.get_level_values(level='timeframe').unique():
            timeframe_pivots = single_timeframe(pivots, timeframe)
            insert_pivot_info(timeframe_pivots, ohlcva, structure_timeframe_shortlist, base_timeframe_ohlcva,
                              timeframe)
    # pivots = insert_pivot_info(pivots, mt_ohlcva, structure_timeframe_shortlist, base_timeframe_ohlcva)
    # pivots = insert_ftc(pivots, mt_ohlcva)
    return pivots


def insert_major_timeframe(pivots, structure_timeframe_shortlist):
    for timeframe in structure_timeframe_shortlist[::-1][:-1]:
        timeframe_pivots = single_timeframe(pivots, timeframe)
        same_time_pattern_timeframe_pivots = pivots[
            pivots.index.get_level_values(level='date').isin(timeframe_pivots.index.get_level_values(level='date'))
            & (pivots.index.get_level_values(level='timeframe') == pattern_timeframe(timeframe))
            ]
        if len(timeframe_pivots) != len(same_time_pattern_timeframe_pivots):
            raise AssertionError("Expected every pivot overlaps with a pivot in pattern time. "
                                 "len(timeframe_pivots) != len(same_time_pattern_timeframe_pivots)")
    pivots['timeframe_timedelta'] = pd.to_timedelta(pivots.index.get_level_values(level='timeframe'))
    unique_date_pivots = pivots.groupby(level='date')['timeframe_timedelta'].idxmax().tolist()
    pivots['major_timeframe'] = False
    pivots.loc[unique_date_pivots, 'major_timeframe'] = True
    if len(pivots[pivots['major_timeframe']]) != len(pivots.index.get_level_values(level='date').unique()):
        raise AssertionError(
            "For each date/time which is a pivot expected to have one and only one major-timeframe pivot."
            "len(pivots[pivots['major_timeframe'] == True]) != pivots.index.get_level_values(level='date').unique()")


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


@measure_time
def insert_more_significant_tops(timeframe_tops: pt.DataFrame[PeakValley],
                                 compared_tops: pt.DataFrame[MultiTimeframePeakValley]) \
        -> pt.DataFrame[MultiTimeframePeakValley]:
    timeframe_tops.drop(
        columns=['right_crossing_time', 'left_crossing_time', 'right_crossing_value', 'left_crossing_value'],
        inplace=True, errors='ignore')
    insert_more_significant_top(timeframe_tops, compared_tops, TopTYPE.PEAK)
    insert_more_significant_top(timeframe_tops, compared_tops, TopTYPE.VALLEY)
    timeframe_tops.rename(columns={
        'right_crossing_time': 'right_more_significant_top',
        'left_crossing_time': 'left_more_significant_top',
        'right_crossing_value': 'right_more_significant_top_value',
        'left_crossing_value': 'left_more_significant_top_value',
    }, inplace=True)
    return timeframe_tops


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
