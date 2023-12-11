# import talib as ta
import os
from typing import Literal

import pandas as pd
import pandera.typing as pt
from pandas._typing import Axes

from Config import config, INFINITY_TIME_DELTA, TopTYPE
from MetaTrader import MT
from Model.MultiTimeframeOHLCV import OHLCV
from Model.MultiTimeframePeakValleys import PeakValleys, MultiTimeframePeakValleys
from data_preparation import read_file, cast_and_validate, trim_to_date_range, \
    expand_date_range, after_under_process_date, empty_df
from helper import measure_time, date_range
from ohlcv import read_base_timeframe_ohlcv


def calculate_strength(peaks_or_valleys: pt.DataFrame[PeakValleys], top_type: TopTYPE,
                       ohlcv: pt.DataFrame[OHLCV]) -> pt.DataFrame[PeakValleys]:
    # todo: test calculate_strength
    if len(peaks_or_valleys) == 0:
        return peaks_or_valleys
    start = ohlcv.index[0]
    end = ohlcv.index[-1]
    peaks_or_valleys = peaks_or_valleys.copy()
    peaks_or_valleys['strength'] = INFINITY_TIME_DELTA
    peaks_or_valleys['start_distance'] = peaks_or_valleys.index - start
    peaks_or_valleys['end_distance'] = end - peaks_or_valleys.index
    peaks_or_valleys = calculate_distance(ohlcv, peaks_or_valleys, top_type, direction='right')
    assert not peaks_or_valleys.index.duplicated(False).any()
    peaks_or_valleys = calculate_distance(ohlcv, peaks_or_valleys, top_type, direction='left')
    assert not peaks_or_valleys.index.duplicated(False).any()
    peaks_or_valleys['strength'] = peaks_or_valleys[['right_distance', 'left_distance']].min(axis='columns')
    assert not peaks_or_valleys.index.duplicated(False).any()
    # peaks_or_valleys['strength'] = fix_same_value_peaks_or_valleys(peaks_or_valleys, top_type)
    tops_with_unknown_strength = peaks_or_valleys[peaks_or_valleys['strength'].isna()]
    # assert len(tops_with_unknown_strength) == 1
    tops_with_strength = peaks_or_valleys[peaks_or_valleys['strength'].notna()]
    assert not tops_with_strength.index.duplicated(False).any()

    peaks_or_valleys.loc[tops_with_strength.index, 'permanent_strength'] = (
            (tops_with_strength['strength'] < tops_with_strength['start_distance']) &
            (tops_with_strength['strength'] < tops_with_strength['end_distance']))
    peaks_or_valleys['strength'] = peaks_or_valleys[['strength', 'start_distance', 'end_distance']].min(axis='columns')
    return peaks_or_valleys


def shift_over(needles: Axes, reference: Axes, side: str, start=None, end=None) -> Axes:
    """
    it will merge the indexes for both forward and backward and return. missing indexes in forward will be filled
    forward and missing indexes of backward will be filled backward.\n
    to find adjacent or nearest PREVIOUS row we can use as:\n
    mapped_list = shift_over(needles, reference, 'forward')\n
    to find adjacent or nearest NEXT row we can use as:\n
    mapped_list = shift_over(needles, reference, 'backward')

    :param needles: needles list
    :param reference: reference list
    :param start: filtering the start of output indexes
    :param end: filtering the rnd of output indexes
    :param side: should be either "forward" or "backward". use "forward" to get the PREVIOUS reference for needles and
    use "backward" to get the NEXT reference for needles
    :return: Axes indexed as the combination of forward and backward indexes and 2 columns:
        'forward': forward input mapped to indexes and forward filled.
        'backward': backward input mapped to indexes and backward filled.

    Example:\n
    reference.index	    1 2 3 5 10 20       \n
    needles.index    	1 2 3 6 9 15 20     \n

    shift_over(needles, reference, 'forward'):
    mapped_list.index   1  2 3 5 6 9  10 15 20      \n
    forward(reference)  NA 1 2 3 5 5  5  10 10      \n
    backward(needles)   2  3 6 6 9 15 15 20 NA      \n
    return:
                        1  2 3 6 9 15 20 \n
                        NA 1 2 5 5 10 10

    shift_over(needles, reference, 'backward'):
    mapped_list.index   1  2 3  5  6  9 10 15 20
    forward(needles)    NA 1 2  3  3  6  9  9 15
    backward(reference) 2  3 5 10 10 10 20 20 NA
    return:
                        1 2 3  6  9 15 20   \n
                        2 3 5 10 10 20 NA
    """
    side = side.lower()
    if side == 'forward':
        forward = reference
        backward = needles
    elif side == 'backward':
        forward = needles
        backward = reference
    else:
        raise Exception('side should be either "forward" or "backward".')
    df = pd.DataFrame(index=forward.append(backward).unique())
    df.sort_index(inplace=True)
    if side == 'forward':
        df.loc[forward, 'forward'] = forward
        df['forward'] = df['forward'].ffill().shift(1)
    elif side == 'backward':
        df.loc[backward, 'backward'] = backward
        df['backward'] = df['backward'].bfill().shift(-1)
    if start is not None:
        df = df[df.index >= start]
    if end is not None:
        df = df[df.index >= end]
    if side == 'forward':
        return df.loc[needles, 'forward'].to_list()
    elif side == 'backward':
        return df.loc[needles, 'backward'].to_list()


def calculate_distance(ohlcv: pt.DataFrame[OHLCV], peaks_or_valleys: pt.DataFrame[PeakValleys], top_type: TopTYPE,
                       direction: Literal['right', 'left']) -> pt.DataFrame[PeakValleys]:
    compare_column, direction, les_significant, more_significant, reverse = direction_parameters(direction, top_type)
    ohlcv = ohlcv.copy()
    tops_to_compare = peaks_or_valleys.copy()
    tops_with_known_crossing_bar = empty_df(PeakValleys)
    number_of_crossed_tops = 1
    while number_of_crossed_tops > 0:
        ohlcv.drop(columns=['right_top_time', 'right_top_value', 'left_top_time', 'left_top_value',
                            'right_crossing', 'left_crossing',
                            'right_crossing_time', 'right_crossing_value',
                            'left_crossing_time', 'left_crossing_value',
                            'valid_crossing'],
                   inplace=True, errors='ignore')
        tops_to_compare.drop(columns=[direction + '_distance'], inplace=True, errors='ignore')

        top_indexes = tops_to_compare.index
        if direction == 'right':
            adjacent_ohlcv_index_of_tops = \
                shift_over(needles=top_indexes, reference=ohlcv.index, side='backward')
        else:  # direction == 'left'
            adjacent_ohlcv_index_of_tops = \
                shift_over(needles=top_indexes, reference=ohlcv.index, side='forward')
        assert len(adjacent_ohlcv_index_of_tops) == len(top_indexes)
        ohlcv.loc[adjacent_ohlcv_index_of_tops, f'{reverse}_top_time'] = top_indexes

        # add the high/low of previous peak/valley to OHLCV df
        ohlcv.loc[adjacent_ohlcv_index_of_tops, reverse + '_top_value'] = \
            tops_to_compare.loc[top_indexes, compare_column].tolist()
        if direction == 'right':
            ohlcv['left_top_time'].ffill(inplace=True)
            ohlcv['left_top_value'].ffill(inplace=True)
        else:  # direction == 'left'
            ohlcv['right_top_time'].bfill(inplace=True)
            ohlcv['right_top_value'].bfill(inplace=True)
        # if high/low of OHLCV is higher/lower than peak/valley high/low it is crossing the peak/valley
        ohlcv[f'{direction}_crossing'] = more_significant(ohlcv[compare_column], ohlcv[reverse + '_top_value'])
        crossing_ohlcv = ohlcv[ohlcv[f'{direction}_crossing'] == True].index

        if direction == 'right':
            shifted_crossing_ohlcv = \
                ohlcv[ohlcv[f'{direction}_crossing'].shift(-1) == True].index
        else:  # direction == 'left'
            shifted_crossing_ohlcv = \
                ohlcv[ohlcv[f'{direction}_crossing'].shift(1) == True].index

        ohlcv.loc[shifted_crossing_ohlcv, f'{direction}_crossing_time'] = pd.to_datetime(crossing_ohlcv)
        ohlcv.loc[shifted_crossing_ohlcv, f'{direction}_crossing_value'] = \
            ohlcv.loc[crossing_ohlcv, compare_column].to_list()
        if direction.lower() == 'left':
            pass
        if direction == 'right':
            ohlcv[f'{direction}_crossing_time'].bfill(inplace=True)
            ohlcv[f'{direction}_crossing_value'].bfill(inplace=True)
        else:  # direction == 'left'
            ohlcv[f'{direction}_crossing_time'].ffill(inplace=True)
            ohlcv[f'{direction}_crossing_value'].ffill(inplace=True)
        ohlcv['masked_ohlcv'] = les_significant(ohlcv[compare_column], ohlcv[f'{direction}_crossing_value'])
        masked_ohlcv = ohlcv[ohlcv['masked_ohlcv'] == True].index
        crossed_tops = masked_ohlcv.intersection(top_indexes)
        number_of_crossed_tops = len(crossed_tops)
        if number_of_crossed_tops > 0:
            tops_to_compare.loc[crossed_tops, direction + '_distance'] = (
                abs(pd.to_datetime(crossed_tops) - ohlcv.loc[crossed_tops, f'{direction}_crossing_time']))
            if len(tops_with_known_crossing_bar) == 0:
                tops_with_known_crossing_bar = tops_to_compare.loc[crossed_tops]
            else:
                tops_with_known_crossing_bar = pd.concat(
                    [tops_with_known_crossing_bar, tops_to_compare.loc[crossed_tops]])
            if tops_with_known_crossing_bar.index.duplicated(keep=False).any():
                raise Exception('Should be unique')
            tops_to_compare.drop(crossed_tops,
                                 inplace=True)  # = tops_to_compare[tops_to_compare[direction + '_distance'].isna()]
    tops_with_known_crossing_bar = pd.concat([tops_with_known_crossing_bar, tops_to_compare]).sort_index()
    assert not tops_with_known_crossing_bar.index.duplicated(keep=False).any()
    assert len(tops_with_known_crossing_bar) == len(peaks_or_valleys)
    return tops_with_known_crossing_bar


def direction_parameters(direction, top_type):
    direction = direction.lower()
    if direction.lower() == 'right':
        reverse = 'left'
        compare_column, les_significant, more_significant = top_operators(top_type)
    elif direction.lower() == 'left':
        reverse = 'right'
        compare_column, les_significant, more_significant = top_operators(top_type, equal_is_significant=True)

    else:
        raise Exception(f'Invalid direction: {direction} only right and left are supported.')
    return compare_column, direction, les_significant, more_significant, reverse


def top_operators(top_type, equal_is_significant: bool = False):
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
    return compare_column, les_significant, more_significant


def map_strength_to_frequency(peaks_valleys: pd.DataFrame) -> pt.DataFrame[PeakValleys]:
    # peaks_valleys.insert(len(peaks_valleys.columns), 'timeframe', None)
    peaks_valleys['timeframe'] = None

    for i in range(len(config.timeframes)):
        for t_peak_valley_index in peaks_valleys[
            peaks_valleys['strength'] > pd.to_timedelta(config.timeframes[i])
        ].index.values:
            peaks_valleys.at[t_peak_valley_index, 'timeframe'] = config.timeframes[i]
    peaks_valleys = peaks_valleys[pd.notna(peaks_valleys['timeframe'])]
    return peaks_valleys


def peaks_only(peaks_n_valleys: pt.DataFrame[PeakValleys]) -> pd.DataFrame:
    """
        Filter peaks from the DataFrame containing peaks and valleys data.

        Parameters:
            peaks_n_valleys (pd.DataFrame): DataFrame containing peaks and valleys data.

        Returns:
            pd.DataFrame: DataFrame containing only peaks data.
    """
    return peaks_n_valleys[peaks_n_valleys['peak_or_valley'] == TopTYPE.PEAK.value]


def valleys_only(peaks_n_valleys: pd.DataFrame) -> pd.DataFrame:
    """
        Filter valleys from the DataFrame containing peaks and valleys data.

        Parameters:
            peaks_n_valleys (pd.DataFrame): DataFrame containing peaks and valleys data.

        Returns:
            pd.DataFrame: DataFrame containing only valleys data.
    """
    return peaks_n_valleys[peaks_n_valleys['peak_or_valley'] == TopTYPE.VALLEY.value]


def merge_tops(peaks: pd.DataFrame, valleys: pd.DataFrame) -> pd.DataFrame:
    return pd.concat([peaks, valleys]).sort_index(level='date')


def find_peaks_n_valleys(base_ohlcv: pd.DataFrame,
                         sort_index: bool = True) -> pt.DataFrame[PeakValleys]:  # , max_cycles=100):
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
    _peaks_n_valleys: pt.DataFrame[PeakValleys] = pd.concat([_peaks, _valleys])
    _peaks_n_valleys = _peaks_n_valleys.loc[:, ['open', 'high', 'low', 'close', 'volume', 'peak_or_valley']]
    return _peaks_n_valleys.sort_index(level='date') if sort_index else _peaks_n_valleys


@measure_time
def major_peaks_n_valleys(multi_timeframe_peaks_n_valleys: pd.DataFrame, timeframe: str) \
        -> pt.DataFrame[MultiTimeframePeakValleys]:
    """
    Filter rows from multi_timeframe_peaks_n_valleys with a timeframe equal to or greater than the specified timeframe.

    Parameters:
        multi_timeframe_peaks_n_valleys (pd.DataFrame): DataFrame containing peaks and valleys data with 'timeframe' index.
        timeframe (str): The timeframe to filter rows.

    Returns:
        pd.DataFrame: DataFrame containing rows with timeframe equal to or greater than the specified timeframe.
    """
    result = higher_or_eq_timeframe_peaks_n_valleys(multi_timeframe_peaks_n_valleys, timeframe)
    return result


def higher_or_eq_timeframe_peaks_n_valleys(peaks_n_valleys: pd.DataFrame, timeframe: str):
    try:
        index = config.timeframes.index(timeframe)
    except ValueError as e:
        raise Exception(f'timeframe:{timeframe} should be in [{config.timeframes}]!')
    except Exception as e:
        raise e
    # result = peaks_n_valleys.loc[peaks_n_valleys.index.isin(config.timeframes[index:], level='timeframe')]
    result = peaks_n_valleys.loc[peaks_n_valleys.index.get_level_values('timeframe').isin(config.timeframes[index:])]
    return result


def top_timeframe(tops: pt.DataFrame[PeakValleys]) -> pt.DataFrame[PeakValleys]:
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


def multi_timeframe_peaks_n_valleys(expanded_date_range: str) -> pt.DataFrame[MultiTimeframePeakValleys]:
    base_ohlcv = read_base_timeframe_ohlcv(expanded_date_range)

    _peaks_n_valleys = find_peaks_n_valleys(base_ohlcv, sort_index=False)

    _peaks_n_valleys = calculate_strength_of_peaks_n_valleys(base_ohlcv, _peaks_n_valleys)

    _peaks_n_valleys = top_timeframe(_peaks_n_valleys)

    _peaks_n_valleys.set_index('timeframe', append=True, inplace=True)
    _peaks_n_valleys = _peaks_n_valleys.swaplevel()
    _peaks_n_valleys = _peaks_n_valleys.sort_index(level='date')

    _peaks_n_valleys = (
        cast_and_validate(_peaks_n_valleys, MultiTimeframePeakValleys,
                          zero_size_allowed=after_under_process_date(expanded_date_range)))
    return _peaks_n_valleys


@measure_time
def generate_multi_timeframe_peaks_n_valleys(date_range_str, file_path: str = config.path_of_data):
    biggest_timeframe = config.timeframes[-1]
    expanded_date_range = expand_date_range(date_range_str,
                                            time_delta=4 * pd.to_timedelta(biggest_timeframe),
                                            mode='both')
    _peaks_n_valleys = multi_timeframe_peaks_n_valleys(expanded_date_range)
    start, end = date_range(date_range_str)
    _peaks_n_valleys = _peaks_n_valleys.loc[
        (start.replace(tzinfo=None) < _peaks_n_valleys.index.get_level_values(level='date')) &
        (_peaks_n_valleys.index.get_level_values(level='date') < end.replace(tzinfo=None))]
    # plot_multi_timeframe_peaks_n_valleys(_peaks_n_valleys, date_range_str)
    _peaks_n_valleys.sort_index(inplace=True, level='date')
    _peaks_n_valleys = trim_to_date_range(date_range_str, _peaks_n_valleys, ignore_duplicate_index=True)
    _peaks_n_valleys = \
        _peaks_n_valleys[['open', 'high', 'low', 'close', 'volume', 'peak_or_valley', 'strength', 'permanent_strength']]
    _peaks_n_valleys.to_csv(os.path.join(file_path, f'multi_timeframe_peaks_n_valleys.{date_range_str}.zip'),
                            compression='zip')
    MT.extract_to_data_path(os.path.join(file_path, f'multi_timeframe_peaks_n_valleys.{date_range_str}.zip'))


@measure_time
def calculate_strength_of_peaks_n_valleys(time_ohlcv: pt.DataFrame[OHLCV],
                                          time_peaks_n_valleys: pt.DataFrame[PeakValleys]) \
        -> pt.DataFrame[PeakValleys]:
    peaks = calculate_strength(peaks_only(time_peaks_n_valleys), TopTYPE.PEAK, time_ohlcv)
    valleys = calculate_strength(valleys_only(time_peaks_n_valleys), TopTYPE.VALLEY, time_ohlcv)
    peaks_and_valleys = pd.concat([peaks, valleys]).sort_index(level='date')
    peaks_and_valleys['strength'] = peaks_and_valleys['strength'].dt.total_seconds()
    return peaks_and_valleys


def read_multi_timeframe_peaks_n_valleys(date_range_str: str = None) \
        -> pt.DataFrame[MultiTimeframePeakValleys]:
    result = read_file(date_range_str, 'multi_timeframe_peaks_n_valleys',
                       generate_multi_timeframe_peaks_n_valleys,
                       MultiTimeframePeakValleys)
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


def insert_previous_n_next_top(top_type, peaks_n_valleys, ohlcv):
    raise Exception('not tested')
    # Define columns
    prev_index_col = f'previous_top_index'
    prev_value_col = f'previous_top_value'
    next_index_col = f'next_top_index'
    next_value_col = f'next_top_value'

    # Ensure columns exist
    for col in [prev_index_col, prev_value_col, next_index_col, next_value_col]:
        if col not in ohlcv.columns:
            ohlcv[col] = None

    # Filter the relevant tops
    tops = peaks_n_valleys[peaks_n_valleys['peak_or_valley'] == top_type.value].copy()

    high_or_low = 'high' if top_type == TopTYPE.PEAK else 'low'

    # Using `shift()` to create the previous and next columns
    tops[prev_index_col] = tops.index.to_series().shift(-1)
    tops[prev_value_col] = tops[high_or_low].shift(-1)
    tops[next_index_col] = tops.index.to_series().shift(1)
    tops[next_value_col] = tops[high_or_low].shift(1)

    # Using `merge_asof` to efficiently merge the previous and next values
    ohlcv = pd.merge_asof(ohlcv.sort_index(), tops[[prev_index_col, prev_value_col]],
                          left_index=True, right_index=True, direction='forward', suffixes=('', '_y'))
    ohlcv = pd.merge_asof(ohlcv.sort_index(), tops[[next_index_col, next_value_col]],
                          left_index=True, right_index=True, direction='backward', suffixes=('', '_y'))

    # Cleaning any duplicate columns
    for col in ohlcv.columns:
        if col.endswith('_y'):
            ohlcv.drop(col, axis=1, inplace=True)

    return ohlcv
