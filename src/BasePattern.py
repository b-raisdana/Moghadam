import os
from datetime import datetime
from typing import List, Literal, Union

import pandas as pd
from pandera import typing as pt, Timestamp

from Config import config, CandleSize
from Model.BasePattern import BasePattern, MultiTimeframeBasePattern
from Model.OHLCVA import OHLCVA, MultiTimeframeOHLCVA
from Model.Pivot import MultiTimeframePivot
from atr import read_multi_timeframe_ohlcva
from data_preparation import single_timeframe, concat, cast_and_validate, empty_df, read_file, anti_pattern_timeframe, \
    anti_trigger_timeframe, to_timeframe


def add_candle_size(ohlcva: pt.DataFrame[OHLCVA]) -> pt.DataFrame[OHLCVA]:
    """
    if the candle       (high - low) <= 80% ATR         the candle is Spinning
    if the candle 80% ATR < (high - low) <= 120% ATR    the candle is Standard
    if the candle 120% ATR < (high - low) <= 250% ATR   the candle is Long
    if the candle 250% ATR < (high - low)               the candle is Spike
    if the candle (high - low) <= 80% ATR the candle is Spinning
    if the candle (high - low) <= 80% ATR the candle is Spinning
    :param ohlcva:
    :return:
    """
    # todo: test candle_size
    assert ohlcva['ATR'].notna().all()
    ohlcva['length'] = ohlcva['high'] - ohlcva['low']
    ohlcva['atr_ratio'] = ohlcva['length'] / ohlcva['ATR']
    ohlcva['size'] = pd.Series(dtype=str)
    ohlcva.loc[(ohlcva['atr_ratio'] <= CandleSize.Spinning.value.max), 'size'] = CandleSize.Spinning.name
    ohlcva.loc[(CandleSize.Standard.value.min < ohlcva['atr_ratio'])
               & (ohlcva['atr_ratio'] <= CandleSize.Standard.value.max), 'size'] = CandleSize.Standard.name
    ohlcva.loc[(CandleSize.Long.value.min < ohlcva['atr_ratio'])
               & (ohlcva['atr_ratio'] <= CandleSize.Long.value.max), 'size'] = CandleSize.Long.name
    ohlcva.loc[(CandleSize.Spike.value.min < ohlcva['atr_ratio']), 'size'] = CandleSize.Spike.name
    return ohlcva


def sequence_of_spinning(ohlcva: pt.DataFrame[OHLCVA], timeframe: str, number_of_base_spinning_candles: int = None) \
        -> pt.Index[Timestamp]:
    """
    Return the index of Spinning candles followed by N Spinning candles.

    Parameters:
    - ohlcva: DataFrame with columns 'open', 'high', 'low', 'close', 'volume', and 'ATR'.

    Example usage:
    Assuming ohlcva is a DataFrame with columns 'open', 'high', 'low', 'close', and 'volume'
    You can call the function like this:
    result = sequence_of_spinning(ohlcva)
    The result will be an Index containing the index of Spinning candles followed by N Spinning candles.

    Returns:
    - Index[Timestamp]: Index of Spinning candles followed by N Spinning candles.
    """
    # todo: test sequence_of_spinning
    if number_of_base_spinning_candles is None:
        number_of_base_spinning_candles = config.base_pattern_number_of_spinning_candles
    if len(ohlcva) < number_of_base_spinning_candles + 1:
        raise ValueError(f"Insufficient data ({len(ohlcva)}) "
                         f"for the specified number of base spinning candles ({number_of_base_spinning_candles})")
    ohlcva = add_candle_size(ohlcva)
    # Check if each candle is spinning
    spinning_candles = ohlcva[ohlcva['size'] == CandleSize.Spinning.name].copy()
    for i in range(1, number_of_base_spinning_candles):
        nth_previous_candle_indexes = spinning_candles.index.shift(-i, freq=timeframe)
        spinning_candles[f'previous_candle_size_{i}'] = ohlcva.loc[nth_previous_candle_indexes, 'size'].tolist()
    candle_size_columns = spinning_candles.filter(like='previous_candle_size_')
    _sequence_of_spinning = (candle_size_columns == CandleSize.Spinning.name).all(axis='columns')
    return spinning_candles[_sequence_of_spinning].index.shift(1, freq='1min')


def base_from_sequence(_sequence_of_spinning: pt.DataFrame[OHLCVA], ohlcva: pt.DataFrame[OHLCVA],
                       number_of_base_spinning_candles: int, timeframe: str) -> pt.DataFrame[BasePattern]:
    """
    Get the index of rows where, in all columns starting with 'next_candle_overlap_',
    their value is below config.base_pattern_candle_min_backward_coverage (80%).

    Parameters:
    - _sequence_of_spinning: DataFrame containing columns starting with 'next_candle_overlap_'.

    Example usage:
    Assuming _sequence_of_spinning is a DataFrame with columns starting with 'next_candle_overlap_'
    You can call the function like this:
    result = base_from_sequence(_sequence_of_spinning)
    The result will be an Index containing the index of rows that satisfy the condition.

    Returns:
    - Index[Timestamp]: Index of rows that satisfy the condition.
    """
    # todo: test  base_from_sequence
    overlap_columns, _sequence_of_spinning = add_previous_candle_overlap(_sequence_of_spinning, ohlcva,
                                                                         number_of_base_spinning_candles, timeframe)

    # Check if all columns are above the threshold
    is_base = (overlap_columns > config.base_pattern_candle_min_backward_coverage).all(axis='columns')

    # timeframe_base_patterns is the next index after enough (config.base_pattern_candle_min_backward_coverage)
    # sequential candles
    timeframe_base_patterns = _sequence_of_spinning[is_base].copy()

    timeframe_base_patterns = add_high_and_low(timeframe_base_patterns, ohlcva, number_of_base_spinning_candles)

    return timeframe_base_patterns

    # # todo: test add_high_and_low
    # for i in range(1, number_of_base_spinning_candles + 1):
    #     df = pd.DataFrame(index=timeframe_base_patterns.index)
    #     df['low'] = ohlcva.loc[timeframe_base_patterns.index.shift(-i), 'low'].tolest()
    #     df['previous_low'] = ohlcva.loc[timeframe_base_patterns.index.shift(-1 - i), 'low'].tolist()
    #     df['max_low'] = df[['low', 'previous_low']].max(axis='columns')
    #     df['high'] = ohlcva.loc[timeframe_base_patterns.index.shift(-i), 'high'].tolist()
    #     df['previous_high'] = ohlcva.loc[timeframe_base_patterns.index.shift(-1 - i), 'high'].tolist()
    #     df['min_high'] = df[['high', 'previous_high']].min(axis='columns')
    #     df['intersected_length'] = df['min_high'] - df['max_low']
    #     assert df['intersected_length'] >= 0
    #     if 'previous_intersected_length' not in df.columns:
    #         df['previous_intersected_length'] = df['intersected_length']
    #     else:
    #         previous_selected_intersection_is_more_significant = \
    #             df[df['intersected_length'] > df['previous_intersected_length']].index


def add_previous_candle_overlap(_sequence_of_spinning, ohlcva, number_of_base_spinning_candles, timeframe):
    # todo: test add_previous_candle_overlap
    ohlcva['previous_low'] = ohlcva['low'].shift(1)
    ohlcva['previous_high'] = ohlcva['high'].shift(1)
    for i in range(1, number_of_base_spinning_candles + 1):
        # if i == 0:
        #     shifted_index = _sequence_of_spinning.index
        # else:
        shifted_index = _sequence_of_spinning.index.shift(-i, freq=timeframe)
        _sequence_of_spinning[f'previous_candle_intersect_low_{i}'] = \
            ohlcva.loc[shifted_index, ['low', 'previous_low']].max(axis='columns').tolist()
        _sequence_of_spinning[f'previous_candle_intersect_high_{i}'] = \
            ohlcva.loc[shifted_index, ['high', 'previous_high']].min(axis='columns').tolist()
        _sequence_of_spinning[f'previous_candle_intersect_length_{i}'] = \
            (_sequence_of_spinning[f'previous_candle_intersect_high_{i}'] -
             _sequence_of_spinning[f'previous_candle_intersect_low_{i}'])
        _sequence_of_spinning[f'previous_candle_overlap_{i}'] = (
                _sequence_of_spinning[f'previous_candle_intersect_length_{i}']
                / ohlcva.loc[shifted_index, 'length'].tolist())
    # Filter columns that start with 'next_candle_overlap_'
    overlap_columns = _sequence_of_spinning.filter(like='previous_candle_overlap_', axis='columns')
    assert overlap_columns.notna().all().all()
    assert (overlap_columns <= 1).all().all()
    return overlap_columns, _sequence_of_spinning


# def nth_candle_overlap(ohlcva, nth_next_candle) -> pt.Series[float]:
#     """
#     Calculate the coverage of the Nth candle after ohlcva indexes with the previous candle.
#
#     The coverage is calculated as max((next_high - previous_high), (previous_low - next_low)) / candle_length
#     where candle_length = high - low.
#
#     Parameters:
#     - ohlcva: DataFrame with columns 'open', 'high', 'low', 'close', and 'volume'.
#     - nth_next_candle: The Nth candle after ohlcva indexes.
#
#     Example usage:
#     Assuming ohlcva is a DataFrame with columns 'open', 'high', 'low', 'close', and 'volume'
#     You can call the function like this:
#     result = nth_candle_overlap(ohlcva, nth_next_candle=3)
#     The result will be a Series with coverage values indexed based on ohlcva.
#
#     Returns:
#     - pt.Series: Series indexed as ohlcva with a column representing the coverage.
#     """
#     # todo: test nth_candle_overlap
#     if nth_next_candle <= 0:
#         raise ValueError("nth_next_candle must be a positive integer")
#     assert (ohlcva['length'] > 0).all()
#     # Shift the DataFrame by nth_next_candle rows to get the Nth next candle
#     shifted_ohlcva = ohlcva.shift(-nth_next_candle)
#     if nth_next_candle - 1 > 0:
#         previous_ohlcva = ohlcva.shift(-(nth_next_candle - 1))
#     else:
#         previous_ohlcva = ohlcva
#     # how much of next[high] is outside previous[high]
#     shifted_ohlcva['high_difference'] = shifted_ohlcva['high'] - previous_ohlcva['high']
#     # how much of next[low] is outside previous[low]
#     shifted_ohlcva['low_difference'] = previous_ohlcva['low'] - shifted_ohlcva['low']
#     # shifted_ohlcva['length'] = shifted_ohlcva['high'] - shifted_ohlcva['low']
#
#     coverage = 1 - shifted_ohlcva[['high_difference', 'low_difference']].max(axis='columns') / shifted_ohlcva['length']
#     return coverage


# def add_sequence_values(ohlcva, high_low: Literal['high', 'low'], number_of_base_spinning_candles: int):
#     # ohlcva[f'previous_candle_{high_low}_0'] = ohlcva[high_low]
#     for i in range(1, number_of_base_spinning_candles + 2):
#         ohlcva[f'previous_candle_{high_low}_{i}'] = ohlcva[high_low].shift(i)
#     return ohlcva


def base_highest_low(timeframe_base_patterns: pt.DataFrame[BasePattern]) -> pt.Series[float]:
    # todo: test base_highest_low
    # ohlcva = add_sequence_values(ohlcva, 'low', number_of_base_spinning_candles)
    _base_highest_low = timeframe_base_patterns.filter(like='previous_candle_intersect_low_').max(axis='columns')
    return _base_highest_low


def base_lowest_high(timeframe_base_patterns: pt.DataFrame[BasePattern]) -> pt.Series[float]:
    # todo: test base_lowest_high
    # ohlcva = add_sequence_values(ohlcva, 'high', number_of_base_spinning_candles)
    _base_lowest_high = timeframe_base_patterns.filter(like='previous_candle_intersect_high_').min(axis='columns')
    return _base_lowest_high


def add_high_and_low(timeframe_base_patterns: pt.DataFrame['BasePattern'], ohlcva: pt.DataFrame['OHLCVA'],
                     number_of_base_spinning_candles: int) -> pt.DataFrame['BasePattern']:
    timeframe_base_patterns['internal_low'] = base_highest_low(timeframe_base_patterns)
    timeframe_base_patterns['internal_high'] = base_lowest_high(timeframe_base_patterns)
    timeframe_base_patterns = update_zero_trigger_candles(timeframe_base_patterns, number_of_base_spinning_candles)

    assert (timeframe_base_patterns['internal_high'] > timeframe_base_patterns['internal_low']).all()
    return timeframe_base_patterns


def timeframe_base_pattern(ohlcva: pt.DataFrame[OHLCVA],
                           a_pattern_ohlcva: pt.DataFrame[OHLCVA],
                           a_trigger_ohlcva: pt.DataFrame[OHLCVA],
                           timeframe: str, number_of_base_spinning_candles: int = None) \
        -> pt.DataFrame[BasePattern]:
    # todo: test  timeframe_base
    if number_of_base_spinning_candles is None:
        number_of_base_spinning_candles = config.base_pattern_number_of_spinning_candles

    # _sequence_of_spinning_indexes is the index of first candle after sequence!
    _sequence_of_spinning_indexes = sequence_of_spinning(ohlcva, timeframe, number_of_base_spinning_candles)
    _sequence_of_spinning = ohlcva.loc[_sequence_of_spinning_indexes].copy()

    timeframe_base_patterns = base_from_sequence(_sequence_of_spinning, ohlcva, number_of_base_spinning_candles,
                                                 timeframe)
    timeframe_base_patterns['ttl'] = \
        timeframe_base_patterns.index + pd.to_timedelta(timeframe) * config.base_pattern_ttl
    timeframe_base_patterns['ATR'] = ohlcva.loc[timeframe_base_patterns.index, 'ATR']
    timeframe_base_patterns['a_pattern_ATR'] = a_pattern_ohlcva.loc[
        to_timeframe(timeframe_base_patterns.index, anti_pattern_timeframe(timeframe)), 'ATR'].tolist()
    timeframe_base_patterns['a_trigger_ATR'] = a_trigger_ohlcva.loc[
        to_timeframe(timeframe_base_patterns.index, anti_trigger_timeframe(timeframe)), 'ATR'].tolist()
    update_band_status(timeframe_base_patterns, ohlcva, direction='upper')
    update_band_status(timeframe_base_patterns, ohlcva, direction='below')
    timeframe_base_patterns = cast_and_validate(timeframe_base_patterns, BasePattern)
    return timeframe_base_patterns


def set_zero_trigger_candle_internal_high_and_lows(timeframe_base_patterns: pt.DataFrame[BasePattern],
                                                   number_of_base_spinning_candles: int) -> pt.DataFrame[BasePattern]:
    # todo: test set_zero_trigger_candle_internal_high_and_lows
    zero_trigger_candle_patterns = timeframe_base_patterns[timeframe_base_patterns['zero_trigger_candle'] == True] \
        .copy()
    intersect_length_columns = zero_trigger_candle_patterns.filter(like='previous_candle_intersect_length_',
                                                                   axis='columns')
    zero_trigger_candle_patterns['min_intersect_length'] = intersect_length_columns.idxmin(axis='columns')
    for i in range(1, number_of_base_spinning_candles + 1):
        overlapped_on_nth_column = zero_trigger_candle_patterns[
            zero_trigger_candle_patterns['min_intersect_length'] == f"previous_candle_intersect_length_{i}"].index
        zero_trigger_candle_patterns.loc[overlapped_on_nth_column, 'internal_low'] = \
            zero_trigger_candle_patterns[f'previous_candle_intersect_low_{i}']
        zero_trigger_candle_patterns.loc[overlapped_on_nth_column, 'internal_high'] = \
            zero_trigger_candle_patterns[f'previous_candle_intersect_high_{i}']
    timeframe_base_patterns.loc[zero_trigger_candle_patterns.index, ['internal_low', 'internal_high']] = \
        zero_trigger_candle_patterns[['internal_low', 'internal_high']]
    return timeframe_base_patterns


def update_zero_trigger_candles(timeframe_base_patterns: pt.DataFrame['BasePattern'],
                                number_of_base_spinning_candles: int) -> pt.DataFrame['BasePattern']:
    # todo: test find_zero_trigger_candles
    zero_trigger_candles = timeframe_base_patterns[
        timeframe_base_patterns['internal_high'] <= timeframe_base_patterns['internal_low']
        ].index
    timeframe_base_patterns['zero_trigger_candle'] = False
    timeframe_base_patterns.loc[zero_trigger_candles, 'zero_trigger_candle'] = True
    timeframe_base_patterns = set_zero_trigger_candle_internal_high_and_lows(timeframe_base_patterns,
                                                                             number_of_base_spinning_candles)
    return timeframe_base_patterns


def multi_timeframe_base_patterns(multi_timeframe_ohlcva: pt.DataFrame[MultiTimeframeOHLCVA],
                                  timeframe_shortlist: List['str'] = None) -> pt.DataFrame[MultiTimeframePivot]:
    # todo: test multi_timeframe_base_patterns
    if timeframe_shortlist is None:
        timeframe_shortlist = config.timeframes
    _multi_timeframe_base_patterns = empty_df(MultiTimeframeBasePattern)
    for timeframe in timeframe_shortlist:
        ohlcva = single_timeframe(multi_timeframe_ohlcva, timeframe)
        a_pattern_ohlcva = single_timeframe(multi_timeframe_ohlcva, anti_pattern_timeframe(timeframe))
        a_trigger_ohlcva = single_timeframe(multi_timeframe_ohlcva, anti_trigger_timeframe(timeframe))
        _timeframe_bases = timeframe_base_pattern(ohlcva, a_pattern_ohlcva, a_trigger_ohlcva, timeframe)
        _timeframe_bases['timeframe'] = timeframe
        _timeframe_bases.set_index('timeframe', append=True, inplace=True)
        _timeframe_bases = _timeframe_bases.swaplevel().sort_index(level='date')
        _multi_timeframe_base_patterns = concat(_multi_timeframe_base_patterns, _timeframe_bases)
    _multi_timeframe_base_patterns = cast_and_validate(_multi_timeframe_base_patterns, MultiTimeframeBasePattern)
    return _multi_timeframe_base_patterns


def generate_multi_timeframe_base_patterns(date_range_str: str = None, file_path: str = config.path_of_data,
                                           timeframe_shortlist: List['str'] = None):
    # todo: test generate_multi_timeframe_base_patterns
    if date_range_str is None:
        date_range_str = config.processing_date_range
    multi_timeframe_ohlcva = read_multi_timeframe_ohlcva(date_range_str)

    base_patterns = multi_timeframe_base_patterns(multi_timeframe_ohlcva,
                                                  timeframe_shortlist=timeframe_shortlist)
    base_patterns.sort_index(inplace=True, level='date')
    base_patterns.to_csv(os.path.join(file_path, f'multi_timeframe_bull_bear_side_trends.{date_range_str}.zip'),
                         compression='zip')


def read_multi_timeframe_base_patterns(date_range_str: str = None) -> pt.DataFrame[MultiTimeframeBasePattern]:
    if date_range_str is None:
        date_range_str = config.processing_date_range
    result = read_file(
        date_range_str,
        'multi_timeframe_base_pattern',
        generate_multi_timeframe_base_patterns,
        MultiTimeframeBasePattern)
    return result


# def first_passed_candle(inactive_upper_band_bases: pt.DataFrame[BasePattern], ohlcva: pt.DataFrame[OHLCVA],
#                         band: Literal['upper', 'below']) -> pt.DataFrame[BasePattern]:
#     # todo: test first_candle_passed_base_margin
#     if band == 'upper':
#         high_low = 'high'
#
#         def expand(x, y):
#             return x + y
#     elif band == 'below':
#         high_low = 'low'
#
#         def expand(x, y):
#             return x - y
#     else:
#         raise Exception(f"band should be 'upper' or 'below' but '{band}' given!")
#
#     return inactive_upper_band_bases


def first_passed_candle(start: datetime, end, level: float, ohlcva: pt.DataFrame[OHLCVA],
                        direction: Literal['upper', 'below']) \
        -> Union[datetime, None]:
    # todo: test first_candle_passed_base_margin
    if direction == 'upper':
        high_low = 'high'

        def compare(x, y):
            return x < y

    elif direction == 'below':
        high_low = 'low'

        def compare(x, y):
            return x > y

    else:
        raise Exception(f"band should be 'upper' or 'below' but '{direction}' given!")
    passing_candles = ohlcva.sort_index(level='date').loc[start:end][compare(level, ohlcva[high_low])]
    if len(passing_candles) > 0:
        return passing_candles.index[0]
    return None


def update_band_status(inactive_bases: pt.DataFrame[BasePattern], ohlcva: pt.DataFrame[OHLCVA],
                       direction: Literal['upper', 'below']) -> pt.DataFrame[BasePattern]:
    if direction == 'upper':
        high_low = 'high'

        def expand(x, y):
            return x + y

    elif direction == 'below':
        high_low = 'low'

        def expand(x, y):
            return x - y
    assert inactive_bases[f'internal_{high_low}'].notna().all()
    if f'{direction}_band_activated' in inactive_bases.columns:
        assert inactive_bases[f'{direction}_band_activated'].isna().all()
    for start, base in inactive_bases.iterrows():
        inactive_bases.loc[start, f'{direction}_band_activated'] = \
            first_passed_candle(start, base['ttl'], expand(base[f'internal_{high_low}'], base['ATR']), ohlcva,
                                direction=direction)
    return inactive_bases


def update_below_band_status(inactive_below_band_bases: pt.DataFrame[BasePattern], ohlcva: pt.DataFrame[OHLCVA]):
    assert inactive_below_band_bases['internal_high'].notna().all()
    if 'upper_band_activated' in inactive_below_band_bases.columns:
        assert inactive_below_band_bases['below_band_activated'].isna().all()
    for start, base in inactive_below_band_bases.iterrows():
        inactive_below_band_bases.loc[start, 'upper_band_activated'] = \
            first_passed_candle(start, base['ttl'], base['internal_low'] - base['ATR'], ohlcva, direction='below')
    return inactive_below_band_bases
