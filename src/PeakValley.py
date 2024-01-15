# import talib as ta
import os
from typing import Literal

import pandas as pd
import pandera.typing as pt

from Config import config, TopTYPE
from FigurePlotter.plotter import INFINITY_TIME_DELTA
from MetaTrader import MT
from PanderaDFM.OHLCV import OHLCV
from PanderaDFM.PeakValley import PeakValley, MultiTimeframePeakValley
from helper.data_preparation import read_file, cast_and_validate, trim_to_date_range, \
    expand_date_range, after_under_process_date, empty_df, nearest_match, concat
from helper.helper import measure_time, date_range
from ohlcv import read_base_timeframe_ohlcv


def calculate_strength(peaks_or_valleys: pt.DataFrame[PeakValley], top_type: TopTYPE,
                       ohlcv: pt.DataFrame[OHLCV]) -> pt.DataFrame[PeakValley]:
    if len(peaks_or_valleys) == 0:
        return peaks_or_valleys
    start = ohlcv.index[0]
    end = ohlcv.index[-1]
    peaks_or_valleys = peaks_or_valleys.copy()
    peaks_or_valleys['strength'] = INFINITY_TIME_DELTA
    peaks_or_valleys['start_distance'] = peaks_or_valleys.index - start
    peaks_or_valleys['end_distance'] = end - peaks_or_valleys.index
    peaks_or_valleys = insert_distance(peaks_or_valleys, ohlcv, top_type, direction='right')
    assert not peaks_or_valleys.index.duplicated(False).any()
    peaks_or_valleys = insert_distance(peaks_or_valleys, ohlcv, top_type, direction='left')
    assert not peaks_or_valleys.index.duplicated(False).any()
    peaks_or_valleys['strength'] = peaks_or_valleys[['right_distance', 'left_distance']].min(axis='columns')
    assert not peaks_or_valleys.index.duplicated(False).any()
    # peaks_or_valleys['strength'] = fix_same_value_peaks_or_valleys(peaks_or_valleys, top_type)
    tops_with_unknown_strength = peaks_or_valleys[peaks_or_valleys['strength'].isna()]
    assert len(tops_with_unknown_strength) <= 1
    tops_with_strength = peaks_or_valleys[peaks_or_valleys['strength'].notna()]
    assert not tops_with_strength.index.duplicated(False).any()

    peaks_or_valleys.loc[tops_with_strength.index, 'permanent_strength'] = (
            (tops_with_strength['strength'] < tops_with_strength['start_distance']) &
            (tops_with_strength['strength'] < tops_with_strength['end_distance']))
    peaks_or_valleys['strength'] = peaks_or_valleys[['strength', 'start_distance', 'end_distance']].min(axis='columns')
    return peaks_or_valleys


def insert_distance(base: pt.DataFrame[PeakValley], target: pt.DataFrame[OHLCV], top_type: TopTYPE,
                    direction: Literal['right', 'left'], base_compare_column: str = None) -> pt.DataFrame[PeakValley]:
    """
    Calculates the distance of OHLCV data points from peaks or valleys in a specified direction.

    Parameters:
    - ohlcv (pt.DataFrame[OHLCV]): DataFrame containing OHLCV data.
    - peaks_or_valleys (pt.DataFrame[PeakValley]): DataFrame containing peak/valley information.
    - top_type (TopTYPE): Enum specifying whether peaks or valleys are considered.
    - direction (Literal['right', 'left']): Direction to calculate distance ('right' for right, 'left' for left).
    - compare_column (str, optional): Column to compare for peak/valley values. Defaults to None.

    Returns:
    - pt.DataFrame[PeakValley]: DataFrame with calculated distances for each peak or valley.

    Columns Added to Returned DataFrame:
    - right_distance or left_distance: Distance of each peak or valley in the specified direction.
    - right_top_time or left_top_time: Time index of the top in the specified direction.
    - right_top_value or left_top_value: Value of the top in the specified direction.
    - right_crossing or left_crossing: Boolean indicating whether OHLCV data is crossing the peak/valley.
    - right_crossing_time or left_crossing_time: Time index where the crossing occurs in the specified direction.
    - right_crossing_value or left_crossing_value: Value of the OHLCV data at the crossing point in the specified direction.
    - valid_crossing: Boolean indicating the validity of the crossing.
    """
    base = insert_crossing(base, target, top_type, direction, base_compare_column)  # todo: test
    valid_crossing_times = base[base[f'{direction}_crossing_time'].notna()].index
    base.loc[valid_crossing_times, direction + '_distance'] = \
        abs(pd.to_datetime(valid_crossing_times) - target.loc[valid_crossing_times, f'{direction}_crossing_time'])
    return base


def insert_crossing(base: pt.DataFrame[PeakValley], target: pt.DataFrame[OHLCV], base_type: TopTYPE,
                    direction: Literal['right', 'left'], cross_direction: Literal['up', 'down'] = 'up',
                    base_compare_column: str = None) -> pt.DataFrame[PeakValley]:
    """
    find the fist crossing candle in 'target' which crosses 'base['base_compare_column']'  toward the 'direction'.
    according to 'base_type'=high/low it will consider to find first crossing up/down candle.

    Parameters:
    - target (pt.DataFrame[OHLCV]): DataFrame containing OHLCV data.
    - base (pt.DataFrame[PeakValley]): DataFrame containing peak/valley information.
    - base_type (TopTYPE): Enum specifying whether peaks or valleys are considered.
    - direction (Literal['right', 'left']): Direction to calculate distance ('right' for right, 'left' for left).
    - compare_column (str, optional): Column to compare for peak/valley values. Defaults to None.

    Returns:
    - pt.DataFrame[PeakValley]: DataFrame with calculated distances for each peak or valley.

    Columns Added to Returned DataFrame:
    - right_base_time or left_base_time: Time index of the base in the specified direction.
    - right_base_value or left_base_value: Value of the base in the specified direction.
    - right_crossing or left_crossing: Boolean indicating whether OHLCV data is crossing the peak/valley.
    - right_crossing_time or left_crossing_time: Time index where the crossing occurs in the specified direction.
    - right_crossing_value or left_crossing_value: Value of the OHLCV data at the crossing point in the specified direction.
    - valid_crossing: Boolean indicating the validity of the crossing.
    """
    target_compare_column, direction, les_significant, more_significant, reverse = direction_parameters(direction,
                                                                                                        base_type,
                                                                                                        cross_direction)
    if base_compare_column is None:
        base_compare_column = target_compare_column
    target = target.copy()
    if f'{direction}_crossing_time' not in base.columns:
        base[f'{direction}_crossing_time'] = pd.Series(dtype='datetime64[ns, UTC]')
    if f'{direction}_crossing_value' not in base.columns:
        base[f'{direction}_crossing_value'] = pd.Series(dtype=float)
    bases_to_compare = base.copy()
    bases_with_known_crossing_target = empty_df(PeakValley)
    number_of_crossed_bases = 1
    while number_of_crossed_bases > 0:
        target = target.drop(columns=['right_base_time', 'right_base_value', 'left_base_time', 'left_base_value',
                                      'right_crossing', 'left_crossing',
                                      'right_crossing_time', 'right_crossing_value',
                                      'left_crossing_time', 'left_crossing_value',
                                      'valid_crossing'], errors='ignore')
        bases_to_compare = bases_to_compare.drop(columns=[direction + '_distance'], errors='ignore')

        base_indexes = bases_to_compare.index
        if direction == 'right':
            adjacent_target_index = \
                nearest_match(needles=base_indexes, reference=target.index, direction='backward')
        else:  # direction == 'left'
            adjacent_target_index = \
                nearest_match(needles=base_indexes, reference=target.index, direction='forward')
        assert len(adjacent_target_index) == len(base_indexes)
        target.loc[adjacent_target_index, f'{reverse}_base_time'] = base_indexes
        # add the high/low of previous peak/valley to OHLCV df
        target.loc[adjacent_target_index, reverse + '_base_value'] = \
            bases_to_compare.loc[base_indexes, base_compare_column].tolist()
        if direction == 'right':
            target['left_base_time'] = target['left_base_time'].ffill()
            target['left_base_value'] = target['left_base_value'].ffill()
        else:  # direction == 'left'
            target['right_base_time'] = target['right_base_time'].bfill()
            target['right_base_value'] = target['right_base_value'].bfill()
        # if high/low of 'target' is higher/lower than 'base' high/low it is crossing
        target[f'{direction}_crossing'] = \
            more_significant(target[target_compare_column], target[reverse + '_base_value'])
        crossing_targets = target[target[f'{direction}_crossing'] == True].index
        if direction == 'right':
            shifted_crossing_target = \
                target[target[f'{direction}_crossing'].shift(-1) == True].index
        else:  # direction == 'left'
            shifted_crossing_target = \
                target[target[f'{direction}_crossing'].shift(1) == True].index

        target.loc[shifted_crossing_target, f'{direction}_crossing_time'] = \
            pd.to_datetime(crossing_targets.get_level_values(level='date'))
        target.loc[shifted_crossing_target, f'{direction}_crossing_value'] = \
            target.loc[crossing_targets, target_compare_column].to_list()
        if direction == 'right':
            target[f'{direction}_crossing_time'] = target[f'{direction}_crossing_time'].bfill()
            target[f'{direction}_crossing_value'] = target[f'{direction}_crossing_value'].bfill()
        else:  # direction == 'left'
            target[f'{direction}_crossing_time'] = target[f'{direction}_crossing_time'].ffill()
            target[f'{direction}_crossing_value'] = target[f'{direction}_crossing_value'].ffill()
        target['masked_target'] = les_significant(target[target_compare_column], target[f'{direction}_crossing_value'])
        target['masked_target'] = les_significant(target[target_compare_column], target[f'{direction}_crossing_value'])
        masked_target = target[target['masked_target'] == True].index
        crossed_bases = nearest_match(masked_target, base_indexes, )
        crossed_bases = masked_target.intersection(base_indexes)
        number_of_crossed_bases = len(crossed_bases)
        if number_of_crossed_bases > 0:
            base.loc[crossed_bases, f'{direction}_crossing_time'] = \
                target.loc[crossed_bases, f'{direction}_crossing_time']
            base.loc[crossed_bases, f'{direction}_crossing_value'] = \
                target.loc[crossed_bases, f'{direction}_crossing_value']
            if len(bases_with_known_crossing_target) == 0:
                bases_with_known_crossing_target = bases_to_compare.loc[crossed_bases]
            else:
                bases_with_known_crossing_target = concat(
                    bases_with_known_crossing_target, bases_to_compare.loc[crossed_bases])
            if bases_with_known_crossing_target.index.duplicated(keep=False).any():
                raise Exception('Should be unique')
            bases_to_compare = bases_to_compare.drop(crossed_bases)
    compared_bases = concat(bases_with_known_crossing_target, bases_to_compare).sort_index()
    assert not compared_bases.index.duplicated(keep=False).any()
    assert len(compared_bases) == len(base)
    return base


# def zz_insert_distance(base: pt.DataFrame[PeakValley], target: pt.DataFrame[OHLCV], top_type: TopTYPE,
#                        direction: Literal['right', 'left'], base_compare_column: str = None) -> pt.DataFrame[
#     def calculate_distance(ohlcv: pt.DataFrame[OHLCV], peaks_or_valleys: pt.DataFrame[PeakValley], top_type: TopTYPE,
#                        direction: Literal['right', 'left'], compare_column: str = None) -> pt.DataFrame[PeakValley]:
#     """
#     Calculates the distance of OHLCV data points from peaks or valleys in a specified direction.
#
#     Parameters:
#     - ohlcv (pt.DataFrame[OHLCV]): DataFrame containing OHLCV data.
#     - peaks_or_valleys (pt.DataFrame[PeakValley]): DataFrame containing peak/valley information.
#     - top_type (TopTYPE): Enum specifying whether peaks or valleys are considered.
#     - direction (Literal['right', 'left']): Direction to calculate distance ('right' for right, 'left' for left).
#     - compare_column (str, optional): Column to compare for peak/valley values. Defaults to None.
#
#     Returns:
#     - pt.DataFrame[PeakValley]: DataFrame with calculated distances for each peak or valley.
#
#     Columns Added to Returned DataFrame:
#     - right_distance or left_distance: Distance of each peak or valley in the specified direction.
#     - right_top_time or left_top_time: Time index of the top in the specified direction.
#     - right_top_value or left_top_value: Value of the top in the specified direction.
#     - right_crossing or left_crossing: Boolean indicating whether OHLCV data is crossing the peak/valley.
#     - right_crossing_time or left_crossing_time: Time index where the crossing occurs in the specified direction.
#     - right_crossing_value or left_crossing_value: Value of the OHLCV data at the crossing point in the specified direction.
#     - valid_crossing: Boolean indicating the validity of the crossing.
#     """
#     t_compare_column, direction, les_significant, more_significant, reverse = direction_parameters(direction, top_type)
#     if compare_column is None:
#         compare_column = t_compare_column
#     ohlcv = ohlcv.copy()
#     tops_to_compare = peaks_or_valleys.copy()
#     tops_with_known_crossing_bar = empty_df(PeakValley)
#     number_of_crossed_tops = 1
#     while number_of_crossed_tops > 0:
#         ohlcv = ohlcv.drop(columns=['right_top_time', 'right_top_value', 'left_top_time', 'left_top_value',
#                                     'right_crossing', 'left_crossing',
#                                     'right_crossing_time', 'right_crossing_value',
#                                     'left_crossing_time', 'left_crossing_value',
#                                     'valid_crossing'], errors='ignore')
#         tops_to_compare = tops_to_compare.drop(columns=[direction + '_distance'], errors='ignore')
#
#         top_indexes = tops_to_compare.index
#         if direction == 'right':
#             adjacent_ohlcv_index_of_tops = \
#                 shift_over(needles=top_indexes, reference=ohlcv.index, side='backward')
#         else:  # direction == 'left'
#             adjacent_ohlcv_index_of_tops = \
#                 shift_over(needles=top_indexes, reference=ohlcv.index, side='forward')
#         assert len(adjacent_ohlcv_index_of_tops) == len(top_indexes)
#         ohlcv.loc[adjacent_ohlcv_index_of_tops, f'{reverse}_top_time'] = top_indexes
#         # add the high/low of previous peak/valley to OHLCV df
#         ohlcv.loc[adjacent_ohlcv_index_of_tops, reverse + '_top_value'] = \
#             tops_to_compare.loc[top_indexes, compare_column].tolist()
#         if direction == 'right':
#             ohlcv['left_top_time'] = ohlcv['left_top_time'].ffill()
#             ohlcv['left_top_value'] = ohlcv['left_top_value'].ffill()
#         else:  # direction == 'left'
#             ohlcv['right_top_time'] = ohlcv['right_top_time'].bfill()
#             ohlcv['right_top_value'] = ohlcv['right_top_value'].bfill()
#         # if high/low of OHLCV is higher/lower than peak/valley high/low it is crossing the peak/valley
#         ohlcv[f'{direction}_crossing'] = more_significant(ohlcv[compare_column], ohlcv[reverse + '_top_value'])
#         crossing_ohlcv = ohlcv[ohlcv[f'{direction}_crossing'] == True].index
#
#         if direction == 'right':
#             shifted_crossing_ohlcv = \
#                 ohlcv[ohlcv[f'{direction}_crossing'].shift(-1) == True].index
#         else:  # direction == 'left'
#             shifted_crossing_ohlcv = \
#                 ohlcv[ohlcv[f'{direction}_crossing'].shift(1) == True].index
#
#         ohlcv.loc[shifted_crossing_ohlcv, f'{direction}_crossing_time'] = pd.to_datetime(crossing_ohlcv)
#         ohlcv.loc[shifted_crossing_ohlcv, f'{direction}_crossing_value'] = \
#             ohlcv.loc[crossing_ohlcv, compare_column].to_list()
#         if direction.lower() == 'left':
#             pass
#         if direction == 'right':
#             ohlcv[f'{direction}_crossing_time'] = ohlcv[f'{direction}_crossing_time'].bfill()
#             ohlcv[f'{direction}_crossing_value'] = ohlcv[f'{direction}_crossing_value'].bfill()
#         else:  # direction == 'left'
#             ohlcv[f'{direction}_crossing_time'] = ohlcv[f'{direction}_crossing_time'].ffill()
#             ohlcv[f'{direction}_crossing_value'] = ohlcv[f'{direction}_crossing_value'].ffill()
#         ohlcv['masked_ohlcv'] = les_significant(ohlcv[compare_column], ohlcv[f'{direction}_crossing_value'])
#         masked_ohlcv = ohlcv[ohlcv['masked_ohlcv'] == True].index
#         crossed_tops = masked_ohlcv.intersection(top_indexes)
#         number_of_crossed_tops = len(crossed_tops)
#         if number_of_crossed_tops > 0:
#             tops_to_compare.loc[crossed_tops, direction + '_distance'] = (
#                 abs(pd.to_datetime(crossed_tops) - ohlcv.loc[crossed_tops, f'{direction}_crossing_time']))
#             if len(tops_with_known_crossing_bar) == 0:
#                 tops_with_known_crossing_bar = tops_to_compare.loc[crossed_tops]
#             else:
#                 tops_with_known_crossing_bar = concat(
#                     tops_with_known_crossing_bar, tops_to_compare.loc[crossed_tops])
#             if tops_with_known_crossing_bar.index.duplicated(keep=False).any():
#                 raise Exception('Should be unique')
#             tops_to_compare = tops_to_compare.drop(crossed_tops)
#     tops_with_known_crossing_bar = concat(tops_with_known_crossing_bar, tops_to_compare).sort_index()
#     assert not tops_with_known_crossing_bar.index.duplicated(keep=False).any()
#     assert len(tops_with_known_crossing_bar) == len(peaks_or_valleys)
#     return tops_with_known_crossing_bar


def direction_parameters(direction, top_type, cross_direction: Literal['up', 'down']):
    """
    if direction.lower() == 'right':
        reverse = 'left'
        compare_column, les_significant, more_significant = top_operators(top_type)
    elif direction.lower() == 'left':
        reverse = 'right'
        compare_column, les_significant, more_significant = top_operators(top_type, equal_is_significant=True)

    top_operators(top_type, equal_is_significant=True):
            if equal_is_significant:
        if top_type == TopTYPE.PEAK:
            compare_column = 'high'

            def more_significant(x, y):
                return x >= y

            def les_significant(x, y):
                return x <= y
        else:
            compare_column = 'low'

            def more_significant(x, y):
                return x <= y

            def les_significant(x, y):
                return x >= y
    else:
        if top_type == TopTYPE.PEAK:
            compare_column = 'high'

            def more_significant(x, y):
                return x > y

            def les_significant(x, y):
                return x < y
        else:
            compare_column = 'low'

            def more_significant(x, y):
                return x < y

            def les_significant(x, y):
                return x > y
    :param cross_direction:
    :param direction:
    :param top_type:
    :return:
    """
    direction = direction.lower()
    if direction.lower() == 'right':
        reverse = 'left'
        compare_column, les_significant, more_significant = top_operators(top_type, cross_direction)
    elif direction.lower() == 'left':
        reverse = 'right'
        compare_column, les_significant, more_significant = \
            top_operators(top_type, cross_direction, equal_is_significant=True)

    else:
        raise Exception(f'Invalid direction: {direction} only right and left are supported.')
    return compare_column, direction, les_significant, more_significant, reverse


def top_operators(top_type, cross_direction: Literal['up', 'down'] = 'up', equal_is_significant: bool = False):
    if equal_is_significant:
        if top_type == TopTYPE.PEAK:
            compare_column = 'high'

            def gt(x, y):
                return x >= y

            def lt(x, y):
                return x <= y
        else:
            compare_column = 'low'

            def gt(x, y):
                return x <= y

            def lt(x, y):
                return x >= y
    else:
        if top_type == TopTYPE.PEAK:
            compare_column = 'high'

            def gt(x, y):
                return x > y

            def lt(x, y):
                return x < y
        else:
            compare_column = 'low'

            def gt(x, y):
                return x < y

            def lt(x, y):
                return x > y
    if cross_direction == 'up':
        more_significant = gt
        les_significant = lt
    elif cross_direction == 'down':
        more_significant = gt
        les_significant = lt
    else:
        raise ValueError(f"invalid cross_direction:{cross_direction}")
    return compare_column, les_significant, more_significant


def map_strength_to_frequency(peaks_valleys: pd.DataFrame) -> pt.DataFrame[PeakValley]:
    # peaks_valleys.insert(len(peaks_valleys.columns), 'timeframe', None)
    peaks_valleys['timeframe'] = None

    for i in range(len(config.timeframes)):
        for t_peak_valley_index in peaks_valleys[
            peaks_valleys['strength'] > pd.to_timedelta(config.timeframes[i])
        ].index.values:
            peaks_valleys.at[t_peak_valley_index, 'timeframe'] = config.timeframes[i]
    peaks_valleys = peaks_valleys[pd.notna(peaks_valleys['timeframe'])]
    return peaks_valleys


def peaks_only(peaks_n_valleys: pt.DataFrame[PeakValley]) -> pt.DataFrame[PeakValley]:
    """
        Filter peaks from the DataFrame containing peaks and valleys data.

        Parameters:
            peaks_n_valleys (pd.DataFrame): DataFrame containing peaks and valleys data.

        Returns:
            pd.DataFrame: DataFrame containing only peaks data.
    """
    return peaks_n_valleys[peaks_n_valleys['peak_or_valley'] == TopTYPE.PEAK.value]


def valleys_only(peaks_n_valleys: pd.DataFrame) -> pt.DataFrame[PeakValley]:
    """
        Filter valleys from the DataFrame containing peaks and valleys data.

        Parameters:
            peaks_n_valleys (pd.DataFrame): DataFrame containing peaks and valleys data.

        Returns:
            pd.DataFrame: DataFrame containing only valleys data.
    """
    return peaks_n_valleys[peaks_n_valleys['peak_or_valley'] == TopTYPE.VALLEY.value]


def merge_tops(peaks: pd.DataFrame, valleys: pd.DataFrame) -> pd.DataFrame:
    return concat(peaks, valleys).sort_index(level='date')


def find_peaks_n_valleys(base_ohlcv: pd.DataFrame,
                         sort_index: bool = True) -> pt.DataFrame[PeakValley]:  # , max_cycles=100):
    mask_of_sequence_of_same_value = (base_ohlcv['high'] == base_ohlcv['high'].shift(1))
    list_of_same_high_lows_sequence = base_ohlcv.loc[mask_of_sequence_of_same_value].index

    none_repeating_ohlcv = base_ohlcv.drop(list_of_same_high_lows_sequence)
    mask_of_peaks = (none_repeating_ohlcv['high'] > none_repeating_ohlcv['high'].shift(1)) & (
            none_repeating_ohlcv['high'] > none_repeating_ohlcv['high'].shift(-1))
    _peaks = none_repeating_ohlcv.loc[mask_of_peaks].copy()
    _peaks['peak_or_valley'] = TopTYPE.PEAK.value

    mask_of_sequence_of_same_value = (base_ohlcv['low'] == base_ohlcv['low'].shift(1))
    list_of_same_high_lows_sequence = base_ohlcv.loc[mask_of_sequence_of_same_value].index
    none_repeating_ohlcv = base_ohlcv.drop(list_of_same_high_lows_sequence)

    mask_of_valleys = (none_repeating_ohlcv['low'] < none_repeating_ohlcv['low'].shift(1)) & (
            none_repeating_ohlcv['low'] < none_repeating_ohlcv['low'].shift(-1))
    _valleys = none_repeating_ohlcv.loc[mask_of_valleys].copy()
    _valleys['peak_or_valley'] = TopTYPE.VALLEY.value
    _peaks_n_valleys: pt.DataFrame[PeakValley] = concat(_peaks, _valleys)
    _peaks_n_valleys = _peaks_n_valleys.loc[:, ['open', 'high', 'low', 'close', 'volume', 'peak_or_valley']]
    return _peaks_n_valleys.sort_index(level='date') if sort_index else _peaks_n_valleys


@measure_time
def major_peaks_n_valleys(_multi_timeframe_peaks_n_valleys: pd.DataFrame, timeframe: str) \
        -> pt.DataFrame[MultiTimeframePeakValley]:
    """
    Filter rows from multi_timeframe_peaks_n_valleys with a timeframe equal to or greater than the specified timeframe.

    Parameters:
        _multi_timeframe_peaks_n_valleys (pd.DataFrame): DataFrame containing peaks and valleys data with 'timeframe' index.
        timeframe (str): The timeframe to filter rows.

    Returns:
        pd.DataFrame: DataFrame containing rows with timeframe equal to or greater than the specified timeframe.
    """
    result = higher_or_eq_timeframe_peaks_n_valleys(_multi_timeframe_peaks_n_valleys, timeframe)
    return result


def higher_or_eq_timeframe_peaks_n_valleys(peaks_n_valleys: pd.DataFrame, timeframe: str):
    try:
        index = config.timeframes.index(timeframe)
    except ValueError:
        raise Exception(f'timeframe:{timeframe} should be in [{config.timeframes}]!')
    # result = peaks_n_valleys.loc[peaks_n_valleys.index.isin(config.timeframes[index:], level='timeframe')]
    result = peaks_n_valleys.loc[peaks_n_valleys.index.get_level_values('timeframe').isin(config.timeframes[index:])]
    return result


def top_timeframe(tops: pt.DataFrame[PeakValley]) -> pt.DataFrame[PeakValley]:
    """
    _peaks_n_valleys['timeframe'] = [strength_to_timeframe(row['strength']) for index, row in
                                     _peaks_n_valleys.iterrows()]
    :param tops:
    :return:
    """
    tops['timeframe'] = config.timeframes[0]
    for _, timeframe in enumerate(config.timeframes[1:]):
        eq_or_higher_timeframe = (tops['strength'] >= pd.to_timedelta(timeframe).total_seconds() * 2)
        tops.loc[eq_or_higher_timeframe, 'timeframe'] = timeframe
        if eq_or_higher_timeframe.any() == 0:
            break
    return tops


def multi_timeframe_peaks_n_valleys(expanded_date_range: str) -> pt.DataFrame[MultiTimeframePeakValley]:
    base_ohlcv = read_base_timeframe_ohlcv(expanded_date_range)

    _peaks_n_valleys = find_peaks_n_valleys(base_ohlcv, sort_index=False)

    _peaks_n_valleys = calculate_strength_of_peaks_n_valleys(base_ohlcv, _peaks_n_valleys)

    _peaks_n_valleys = top_timeframe(_peaks_n_valleys)

    _peaks_n_valleys = _peaks_n_valleys.set_index('timeframe', append=True, )
    _peaks_n_valleys = _peaks_n_valleys.swaplevel()
    _peaks_n_valleys = _peaks_n_valleys.sort_index(level='date')

    _peaks_n_valleys = (
        cast_and_validate(_peaks_n_valleys, MultiTimeframePeakValley,
                          zero_size_allowed=after_under_process_date(expanded_date_range)))
    return _peaks_n_valleys


# @measure_time
def generate_multi_timeframe_peaks_n_valleys(date_range_str, file_path: str = None):
    if file_path is None:
        file_path = config.path_of_data
    biggest_timeframe = config.timeframes[-1]
    expanded_date_range = expand_date_range(date_range_str,
                                            time_delta=4 * pd.to_timedelta(biggest_timeframe),
                                            mode='both')
    _peaks_n_valleys = multi_timeframe_peaks_n_valleys(expanded_date_range)
    start, end = date_range(date_range_str)
    _peaks_n_valleys = _peaks_n_valleys.loc[
        (start < _peaks_n_valleys.index.get_level_values(level='date')) &
        (_peaks_n_valleys.index.get_level_values(level='date') < end)].copy()
    # plot_multi_timeframe_peaks_n_valleys(_peaks_n_valleys, date_range_str)
    _peaks_n_valleys = _peaks_n_valleys.sort_index(level='date')
    _peaks_n_valleys = trim_to_date_range(date_range_str, _peaks_n_valleys, ignore_duplicate_index=True)
    _peaks_n_valleys = \
        _peaks_n_valleys[['open', 'high', 'low', 'close', 'volume', 'peak_or_valley', 'strength', 'permanent_strength']]
    _peaks_n_valleys.to_csv(os.path.join(file_path, f'multi_timeframe_peaks_n_valleys.{date_range_str}.zip'),
                            compression='zip')
    MT.extract_to_data_path(os.path.join(file_path, f'multi_timeframe_peaks_n_valleys.{date_range_str}.zip'))


# @measure_time
def calculate_strength_of_peaks_n_valleys(time_ohlcv: pt.DataFrame[OHLCV],
                                          time_peaks_n_valleys: pt.DataFrame[PeakValley]) \
        -> pt.DataFrame[PeakValley]:
    peaks = calculate_strength(peaks_only(time_peaks_n_valleys), TopTYPE.PEAK, time_ohlcv)
    valleys = calculate_strength(valleys_only(time_peaks_n_valleys), TopTYPE.VALLEY, time_ohlcv)
    peaks_and_valleys = concat(peaks, valleys).sort_index(level='date')
    peaks_and_valleys['strength'] = peaks_and_valleys['strength'].dt.total_seconds()
    return peaks_and_valleys


def read_multi_timeframe_peaks_n_valleys(date_range_str: str = None) \
        -> pt.DataFrame[MultiTimeframePeakValley]:
    result = read_file(date_range_str, 'multi_timeframe_peaks_n_valleys',
                       generate_multi_timeframe_peaks_n_valleys,
                       MultiTimeframePeakValley)
    return result


def old_insert_previous_n_next_top(top_type: TopTYPE, peaks_n_valleys, ohlcv):
    if f'previous_top_index' not in ohlcv.columns:
        ohlcv[f'previous_top_index'] = None
    if f'previous_top_value' not in ohlcv.columns:
        ohlcv[f'previous_top_value'] = None
    if f'next_top_index' not in ohlcv.columns:
        ohlcv[f'next_top_index'] = None
    if f'next_top_value' not in ohlcv.columns:
        ohlcv[f'next_top_value'] = None
    tops = peaks_n_valleys[peaks_n_valleys['peak_or_valley'] == top_type.value]
    high_or_low = 'high' if top_type == TopTYPE.PEAK else 'low'
    for i in range(len(tops)):
        if i == len(tops) - 1:
            is_previous_for_indexes = ohlcv.loc[ohlcv.index > tops.index.get_level_values('date')[i]].index
        else:
            is_previous_for_indexes = ohlcv.loc[(ohlcv.index > tops.index.get_level_values('date')[i]) &
                                                (ohlcv.index <= tops.index.get_level_values('date')[i + 1])].index
        if i == 0:
            is_next_for_indexes = ohlcv.loc[ohlcv.index <= tops.index.get_level_values('date')[i]].index
        else:
            is_next_for_indexes = ohlcv.loc[(ohlcv.index > tops.index.get_level_values('date')[i - 1]) &
                                            (ohlcv.index <= tops.index.get_level_values('date')[i])].index
        ohlcv.loc[is_previous_for_indexes, f'previous_top_index'] = tops.index.get_level_values('date')[i]
        ohlcv.loc[is_previous_for_indexes, f'previous_top_value'] = tops.iloc[i][high_or_low]
        ohlcv.loc[is_next_for_indexes, f'next_top_index'] = tops.index.get_level_values('date')[i]
        ohlcv.loc[is_next_for_indexes, f'next_top_value'] = tops.iloc[i][high_or_low]
    return ohlcv


@measure_time
def insert_previous_n_next_top(top_type, peaks_n_valleys: pt.DataFrame[PeakValley], ohlcv: pt.DataFrame[OHLCV]) \
        -> pt.DataFrame[OHLCV]:
    # Define columns
    prev_index_col = f'previous_{top_type.value}_index'
    prev_value_col = f'previous_{top_type.value}_value'
    next_index_col = f'next_{top_type.value}_index'
    next_value_col = f'next_{top_type.value}_value'
    # Filter the relevant tops
    tops = peaks_n_valleys[peaks_n_valleys['peak_or_valley'] == top_type.value].copy()
    high_or_low = 'high' if top_type == TopTYPE.PEAK else 'low'
    # Using `shift()` to create the previous and next columns
    tops[prev_index_col] = tops.index.get_level_values('date').unique().to_series().shift(1).tolist()
    tops[prev_value_col] = tops[high_or_low].shift(-1)
    previous_tops = pd.DataFrame(index=tops.index.get_level_values('date').shift(1, config.timeframes[0])).sort_index()
    previous_tops[prev_index_col] = tops.index.get_level_values('date').unique().tolist()
    previous_tops[prev_value_col] = tops[high_or_low].tolist()
    # merge the previous and next values
    ohlcv = pd.merge_asof(ohlcv.sort_index(), previous_tops,
                          left_index=True, right_index=True, direction='backward', suffixes=('_x', ''))
    tops[next_index_col] = tops.index.get_level_values('date').unique().to_series().shift(-1).tolist()
    tops[next_value_col] = tops[high_or_low].shift(1)
    next_tops = pd.DataFrame(index=tops.index.get_level_values('date').shift(-1, config.timeframes[0])).sort_index()
    next_tops[next_index_col] = tops.index.get_level_values('date').unique().tolist()
    next_tops[next_value_col] = tops[high_or_low].tolist()
    # merge the previous and next values
    ohlcv = pd.merge_asof(ohlcv.sort_index(), next_tops,
                          left_index=True, right_index=True, direction='forward', suffixes=('_x', ''))
    # Cleaning any duplicate columns
    for col in ohlcv.columns:
        if col.endswith('_x') or col.endswith('_y'):
            raise Exception(f'Duplicate merge on same column:{col}')

    return ohlcv
