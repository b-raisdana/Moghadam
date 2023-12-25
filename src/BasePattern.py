import os
from datetime import timedelta
from typing import List, Literal

import pandas as pd
from pandera import typing as pt, Index, Timestamp

from Config import config, CandleSize
from Model.BasePattern import BasePattern, MultiTimeframeBasePattern
from Model.OHLCVA import OHLCVA, MultiTimeframeOHLCVA
from Model.Pivot import MultiTimeframePivot
from atr import read_multi_timeframe_ohlcva
from data_preparation import single_timeframe, concat, cast_and_validate, empty_df, read_file


def candle_size(ohlcva: pt.DataFrame[OHLCVA]) -> pd.Series(CandleSize):
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
    raise Exception('Not implemented')


def sequence_of_spinning(ohlcva: pt.DataFrame[OHLCVA], timeframe: str, number_of_base_spinning_candles: int = None) \
        -> Index[Timestamp]:
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
    ohlcva['size'] = candle_size(ohlcva)
    # Check if each candle is spinning
    spinning_candles = ohlcva[ohlcva['size'] == CandleSize.Spinning.name].copy()
    for i in range(number_of_base_spinning_candles):
        nth_next_candle_indexes = spinning_candles.index.shift(i, frequency=timeframe)
        spinning_candles[f'next_candle_size_{i}'] = ohlcva.loc[nth_next_candle_indexes, 'size']
    filtered_spinning_candles = spinning_candles.filter(like='next_candle_size_')
    _sequence_of_spinning = (filtered_spinning_candles == CandleSize.Spinning.name).all(axis='columns')
    return _sequence_of_spinning.index


def base_from_sequence(_sequence_of_spinning: pt.DataFrame[OHLCVA], timeframe: str) -> pt.Index[Timestamp]:
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
    if not _sequence_of_spinning.columns.str.startswith('next_candle_overlap_').any():
        raise ValueError("No columns found starting with 'next_candle_overlap_'")

    # Filter columns that start with 'next_candle_overlap_'
    overlap_columns = _sequence_of_spinning.filter(like='next_candle_overlap_', axis='columns')

    # Check if all columns is below the threshold
    is_base = (overlap_columns < config.base_pattern_candle_min_backward_coverage).all(axis='columns')

    # base_index is the next index after enough (config.base_pattern_candle_min_backward_coverage) sequential candles
    base_index = (_sequence_of_spinning.index[is_base]
                  .shift(config.base_pattern_candle_min_backward_coverage + 1, freq=timeframe))
    return base_index


def nth_candle_overlap(ohlcva, nth_next_candle) -> pd.Series[float]:
    """
    Calculate the coverage of the Nth candle after ohlcva indexes with the previous candle.

    The coverage is calculated as max((next_high - previous_high), (previous_low - next_low)) / candle_length
    where candle_length = high - low.

    Parameters:
    - ohlcva: DataFrame with columns 'open', 'high', 'low', 'close', and 'volume'.
    - nth_next_candle: The Nth candle after ohlcva indexes.

    Example usage:
    Assuming ohlcva is a DataFrame with columns 'open', 'high', 'low', 'close', and 'volume'
    You can call the function like this:
    result = nth_candle_overlap(ohlcva, nth_next_candle=3)
    The result will be a Series with coverage values indexed based on ohlcva.

    Returns:
    - pd.Series: Series indexed as ohlcva with a column representing the coverage.
    """
    # todo: test nth_candle_overlap
    if nth_next_candle <= 0:
        raise ValueError("nth_next_candle must be a positive integer")

    # Shift the DataFrame by nth_next_candle rows to get the Nth next candle
    shifted_ohlcva = ohlcva.shift(-nth_next_candle)
    if nth_next_candle - 1 > 0:
        previous_ohlcva = ohlcva.shift(-(nth_next_candle - 1))
    else:
        previous_ohlcva = ohlcva
    # how much of next[high] is outside previous[high]
    high_difference = shifted_ohlcva['high'] - previous_ohlcva['high']
    # how much of next[low] is outside previous[low]
    low_difference = previous_ohlcva['low'] - shifted_ohlcva['low']
    shifted_ohlcva['length'] = shifted_ohlcva['high'] - shifted_ohlcva['low']
    assert (shifted_ohlcva['length'] > 0).all()
    coverage = 1 - (max(high_difference, low_difference) / shifted_ohlcva['length'])
    return coverage


def add_sequence_values(ohlcva, high_low: Literal['high', 'low'], number_of_base_spinning_candles: int):
    # todo: test add_sequence_values
    ohlcva[f'previous_candle_{high_low}_0'] = ohlcva[high_low]
    for i in range(number_of_base_spinning_candles):
        ohlcva[f'previous_candle_{high_low}_{i}'] = ohlcva[high_low].shift(i)
    return ohlcva


def base_highest_low(base_indexes, ohlcva, number_of_base_spinning_candles: int) -> pt.Series[float]:
    # todo: test base_highest_low
    ohlcva = add_sequence_values(ohlcva, 'low', number_of_base_spinning_candles)
    _base_highest_low = ohlcva.loc[base_indexes].filter(like='previous_candle_low_').max(axis='column')
    return _base_highest_low


def base_lowest_high(base_indexes, ohlcva, number_of_base_spinning_candles: int) -> pt.Series[float]:
    # todo: test base_lowest_high
    ohlcva = add_sequence_values(ohlcva, 'high', number_of_base_spinning_candles)
    _base_lowest_high = ohlcva.loc[base_indexes].filter(like='previous_candle_high_').min(axis='column')
    return _base_lowest_high


def timeframe_base_pattern(ohlcva: pt.DataFrame[OHLCVA], timeframe: str, number_of_base_spinning_candles: int = None) \
        -> pt.DataFrame[BasePattern]:
    # todo: test  timeframe_base
    if number_of_base_spinning_candles is None:
        number_of_base_spinning_candles = config.base_pattern_number_of_spinning_candles

    _sequence_of_spinning_indexes = sequence_of_spinning(ohlcva, timeframe)
    _sequence_of_spinning = ohlcva.loc[_sequence_of_spinning_indexes].copy()
    for i in range(number_of_base_spinning_candles):
        _sequence_of_spinning[f'next_candle_overlap_{i}'] = nth_candle_overlap(ohlcva, nth_next_candle=i)
    base_indexes = base_from_sequence(_sequence_of_spinning, timeframe)
    timeframe_base_patterns: pt.DataFrame[BasePattern] = pd.DataFrame(index=base_indexes)
    timeframe_base_patterns['internal_low'] = base_highest_low(base_indexes, ohlcva)
    timeframe_base_patterns['internal_high'] = base_lowest_high(base_indexes, ohlcva)
    timeframe_base_patterns['ttl'] = \
        timeframe_base_patterns.index + pd.to_timedelta(timeframe) * config.base_pattern_ttl
    timeframe_base_patterns = cast_and_validate(timeframe_base_patterns, BasePattern)
    return timeframe_base_patterns


def multi_timeframe_base_patterns(multi_timeframe_ohlcva: pt.DataFrame[MultiTimeframeOHLCVA],
                                  timeframe_shortlist: List['str'] = None) -> pt.DataFrame[MultiTimeframePivot]:
    # todo: test multi_timeframe_base_patterns
    if timeframe_shortlist is None:
        timeframe_shortlist = config.timeframes
    _multi_timeframe_base_patterns = empty_df(MultiTimeframeBasePattern)
    for timeframe in timeframe_shortlist:
        ohlcva = single_timeframe(multi_timeframe_ohlcva, timeframe)
        _timeframe_bases = timeframe_base_pattern(ohlcva, timeframe)
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
