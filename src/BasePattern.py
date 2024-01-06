import os
from datetime import datetime
from typing import List, Literal, Union

import pandas as pd
from pandera import typing as pt, Timestamp

from Config import config, CandleSize
from PanderaDFM.BasePattern import BasePattern, MultiTimeframeBasePattern
from PanderaDFM.OHLCVA import OHLCVA, MultiTimeframeOHLCVA
from PanderaDFM.Pivot import MultiTimeframePivot
from atr import read_multi_timeframe_ohlcva
from helper.data_preparation import single_timeframe, concat, cast_and_validate, empty_df, read_file, \
    anti_pattern_timeframe, \
    anti_trigger_timeframe, to_timeframe, trim_to_date_range
from helper.helper import date_range, date_range_to_string, measure_time


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
    if number_of_base_spinning_candles is None:
        number_of_base_spinning_candles = config.base_pattern_number_of_spinning_candles
    if len(ohlcva) < number_of_base_spinning_candles + 1:
        raise ValueError(f"Insufficient data ({len(ohlcva)}) "
                         f"for the specified number of base spinning candles ({number_of_base_spinning_candles})")
    ohlcva = add_candle_size(ohlcva)
    loop_shift = 0  # config.base_pattern_index_shift_after_last_candle_in_the_sequence todo: may need optimization
    spinning_candles = ohlcva.iloc[number_of_base_spinning_candles + loop_shift - 1:] \
        .loc[ohlcva['size'] == CandleSize.Spinning.name].copy()
    for i in range(loop_shift, number_of_base_spinning_candles + loop_shift):
        nth_previous_candle_indexes = spinning_candles.index.shift(-i, freq=timeframe)
        if timeframe == '15min':
            pass
        spinning_candles[f'previous_candle_size_{i}'] = ohlcva.loc[nth_previous_candle_indexes, 'size'].tolist()
    candle_size_columns = spinning_candles.filter(like='previous_candle_size_')
    _sequence_of_spinning = (candle_size_columns == CandleSize.Spinning.name).all(axis='columns')
    # if config.base_pattern_index_shift_after_last_candle_in_the_sequence == 0:
    return spinning_candles[_sequence_of_spinning].index
    # else:
    #     return spinning_candles[_sequence_of_spinning].index.shift(
    #         config.base_pattern_index_shift_after_last_candle_in_the_sequence, freq='1min')


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
    overlap_columns, _sequence_of_spinning = add_previous_candle_overlap(_sequence_of_spinning, ohlcva,
                                                                         number_of_base_spinning_candles, timeframe)

    # Check if all columns are above the threshold
    is_base = (overlap_columns > config.base_pattern_candle_min_backward_coverage).all(axis='columns')

    # timeframe_base_patterns is the last candle of enough (config.base_pattern_candle_min_backward_coverage)
    # sequential candles
    # if config.base_pattern_index_shift_after_last_candle_in_the_sequence == 0:
    timeframe_base_patterns = _sequence_of_spinning[is_base].copy()
    # else:
    #     timeframe_base_patterns = _sequence_of_spinning[is_base].copy() \
    #         .shift(config.base_pattern_index_shift_after_last_candle_in_the_sequence, freq=timeframe)

    timeframe_base_patterns = add_high_and_low(timeframe_base_patterns, ohlcva, number_of_base_spinning_candles)

    return timeframe_base_patterns


def add_previous_candle_overlap(_sequence_of_spinning, ohlcva, number_of_base_spinning_candles, timeframe):
    ohlcva['previous_low'] = ohlcva['low'].shift(1)
    ohlcva['previous_high'] = ohlcva['high'].shift(1)
    loop_range_shift = 0  # config.base_pattern_index_shift_after_last_candle_in_the_sequence todo: optimize
    for i in range(loop_range_shift, number_of_base_spinning_candles + loop_range_shift):
        if i != 0:
            shifted_index = _sequence_of_spinning.index.shift(-i, freq=timeframe)
        else:
            shifted_index = _sequence_of_spinning.index
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
        _sequence_of_spinning.loc[
            _sequence_of_spinning[f'previous_candle_intersect_length_{i}'] == 0, f'previous_candle_overlap_{i}'] = 0
    # Filter columns that start with 'next_candle_overlap_'
    overlap_columns = _sequence_of_spinning.filter(like='previous_candle_overlap_', axis='columns')
    assert overlap_columns.notna().all().all()
    assert (overlap_columns <= 1).all().all()
    return overlap_columns, _sequence_of_spinning


def base_highest_low(timeframe_base_patterns: pt.DataFrame[BasePattern]) -> pt.Series[float]:
    _base_highest_low = timeframe_base_patterns.filter(like='previous_candle_intersect_low_').max(axis='columns')
    return _base_highest_low


def base_lowest_high(timeframe_base_patterns: pt.DataFrame[BasePattern]) -> pt.Series[float]:
    _base_lowest_high = timeframe_base_patterns.filter(like='previous_candle_intersect_high_').min(axis='columns')
    return _base_lowest_high


def add_high_and_low(timeframe_base_patterns: pt.DataFrame['BasePattern'], ohlcva: pt.DataFrame['OHLCVA'],
                     number_of_base_spinning_candles: int) -> pt.DataFrame['BasePattern']:
    timeframe_base_patterns['internal_low'] = base_highest_low(timeframe_base_patterns)
    timeframe_base_patterns['internal_high'] = base_lowest_high(timeframe_base_patterns)
    timeframe_base_patterns = update_zero_trigger_candles(timeframe_base_patterns, number_of_base_spinning_candles)

    assert (timeframe_base_patterns['internal_high'] > timeframe_base_patterns['internal_low']).all()
    return timeframe_base_patterns


@measure_time
def timeframe_base_pattern(ohlcva: pt.DataFrame[OHLCVA], a_pattern_ohlcva: pt.DataFrame[OHLCVA],
                           a_trigger_ohlcva: pt.DataFrame[OHLCVA], timeframe: str,
                           number_of_base_spinning_candles: int = None) -> pt.DataFrame[BasePattern]:
    if number_of_base_spinning_candles is None:
        number_of_base_spinning_candles = config.base_pattern_number_of_spinning_candles

    _sequence_of_spinning_indexes = sequence_of_spinning(ohlcva, timeframe, number_of_base_spinning_candles)
    _sequence_of_spinning = ohlcva.loc[_sequence_of_spinning_indexes].copy()

    timeframe_base_patterns = base_from_sequence(_sequence_of_spinning, ohlcva, number_of_base_spinning_candles,
                                                 timeframe)
    timeframe_base_patterns['ttl'] = \
        timeframe_base_patterns.index + pd.to_timedelta(timeframe) * config.base_pattern_ttl
    if timeframe == '1h':
        pass
    timeframe_base_patterns['ATR'] = ohlcva.loc[timeframe_base_patterns.index, 'ATR'].tolist()
    a_pattern_times = to_timeframe(timeframe_base_patterns.index, anti_pattern_timeframe(timeframe))
    timeframe_base_patterns['a_pattern_ATR'] = a_pattern_ohlcva.loc[a_pattern_times, 'ATR'].tolist()
    a_trigger_times = to_timeframe(timeframe_base_patterns.index, anti_trigger_timeframe(timeframe))
    timeframe_base_patterns['a_trigger_ATR'] = a_trigger_ohlcva.loc[a_trigger_times, 'ATR'].tolist()
    update_band_status(timeframe_base_patterns, ohlcva, timeframe, direction='upper')
    update_band_status(timeframe_base_patterns, ohlcva, timeframe, direction='below')
    timeframe_base_patterns['end'] = pd.Series(dtype='datetime64[ns, UTC]')
    try:
        timeframe_base_patterns = cast_and_validate(timeframe_base_patterns, BasePattern, zero_size_allowed=True)
    except Exception as e:
        raise e
    return timeframe_base_patterns


def set_zero_trigger_candle_internal_high_and_lows(timeframe_base_patterns: pt.DataFrame[BasePattern],
                                                   number_of_base_spinning_candles: int) -> pt.DataFrame[BasePattern]:
    zero_trigger_candle_patterns = timeframe_base_patterns[timeframe_base_patterns['zero_trigger_candle'] == True] \
        .copy()
    intersect_length_columns = zero_trigger_candle_patterns.filter(like='previous_candle_intersect_length_',
                                                                   axis='columns')
    zero_trigger_candle_patterns['min_intersect_length'] = intersect_length_columns.idxmin(axis='columns')
    loop_shift = 0  # config.base_pattern_index_shift_after_last_candle_in_the_sequence  todo: optimize
    for i in range(loop_shift, number_of_base_spinning_candles + loop_shift):
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
    zero_trigger_candles = timeframe_base_patterns[
        timeframe_base_patterns['internal_high'] <= timeframe_base_patterns['internal_low']
        ].index
    timeframe_base_patterns['zero_trigger_candle'] = False
    timeframe_base_patterns.loc[zero_trigger_candles, 'zero_trigger_candle'] = True
    timeframe_base_patterns = set_zero_trigger_candle_internal_high_and_lows(timeframe_base_patterns,
                                                                             number_of_base_spinning_candles)
    return timeframe_base_patterns


def multi_timeframe_base_patterns(expanded_multi_timeframe_ohlcva: pt.DataFrame[MultiTimeframeOHLCVA],
                                  timeframe_shortlist: List['str'] = None) -> pt.DataFrame[MultiTimeframePivot]:
    if timeframe_shortlist is None:
        # the last 3 timeframes will not have an anti_trigger_timeframe!
        timeframe_shortlist = config.timeframes[:-2]
    else:
        if any([t in timeframe_shortlist for t in config.timeframes[-2:]]):
            raise Exception(f"timeframes {timeframe_shortlist} should have Anti-Trigger time!")
    _multi_timeframe_base_patterns = empty_df(MultiTimeframeBasePattern)
    for timeframe in timeframe_shortlist:
        ohlcva = single_timeframe(expanded_multi_timeframe_ohlcva, timeframe)
        a_pattern_ohlcva = single_timeframe(expanded_multi_timeframe_ohlcva, anti_pattern_timeframe(timeframe))
        a_trigger_ohlcva = single_timeframe(expanded_multi_timeframe_ohlcva, anti_trigger_timeframe(timeframe))
        _timeframe_bases = timeframe_base_pattern(ohlcva, a_pattern_ohlcva, a_trigger_ohlcva, timeframe)
        _timeframe_bases['timeframe'] = timeframe
        _timeframe_bases.set_index('timeframe', append=True, inplace=True)
        _timeframe_bases = _timeframe_bases.swaplevel().sort_index(level='date')
        _multi_timeframe_base_patterns = concat(_multi_timeframe_base_patterns, _timeframe_bases)
    _multi_timeframe_base_patterns = cast_and_validate(_multi_timeframe_base_patterns, MultiTimeframeBasePattern)
    return _multi_timeframe_base_patterns


def generate_multi_timeframe_base_patterns(date_range_str: str = None, file_path: str = config.path_of_data,
                                           timeframe_shortlist: List['str'] = None):
    if date_range_str is None:
        date_range_str = config.processing_date_range
    if timeframe_shortlist is None:
        # the last 3 timeframes will not have an anti_trigger_timeframe!
        timeframe_shortlist = config.timeframes[:-2]
    else:
        if any([t in timeframe_shortlist for t in config.timeframes[-2:]]):
            raise Exception(f"timeframes {timeframe_shortlist} should have Anti-Trigger time!")
    start, end = date_range(date_range_str)
    expanded_start = to_timeframe(start, anti_trigger_timeframe(timeframe_shortlist[-1]), ignore_cached_times=True)
    expanded_date_range_str = date_range_to_string(end=end, start=expanded_start)
    multi_timeframe_ohlcva = read_multi_timeframe_ohlcva(expanded_date_range_str)
    base_patterns = multi_timeframe_base_patterns(multi_timeframe_ohlcva,
                                                  timeframe_shortlist=timeframe_shortlist)
    base_patterns = trim_to_date_range(date_range_str, base_patterns)
    base_patterns.sort_index(inplace=True, level='date')
    base_patterns.to_csv(os.path.join(file_path, f'multi_timeframe_base_pattern.{date_range_str}.zip'),
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


def first_passed_candle(base_index: datetime, end, level: float, ohlcva: pt.DataFrame[OHLCVA], timeframe,
                        direction: Literal['upper', 'below']) \
        -> Union[datetime, None]:
    if direction == 'upper':
        high_low = 'low'  # below ATR should activate upper band

        def compare(x, y):
            return x > y
    elif direction == 'below':
        high_low = 'high'  # above ATR should activate bellow band

        def compare(x, y):
            return x < y
    else:
        raise Exception(f"band should be 'upper' or 'below' but '{direction}' given!")
    start = base_index + pd.to_timedelta(
        timeframe) * config.base_pattern_index_shift_after_last_candle_in_the_sequence
    passing_candles = ohlcva.sort_index(level='date').loc[start:end].loc[compare(level, ohlcva[high_low])]
    if len(passing_candles) > 0:
        return passing_candles.index[0]
    return None


def update_band_status(inactive_bases: pt.DataFrame[BasePattern], ohlcva: pt.DataFrame[OHLCVA], timeframe,
                       direction: Literal['upper', 'below']) -> pt.DataFrame[BasePattern]:
    if direction == 'upper':
        high_low = 'low'  # below ATR should activate upper band

        def expand(x, y):
            return x - y
    elif direction == 'below':
        high_low = 'high'  # above ATR should activate upper band

        def expand(x, y):
            return x + y
    else:
        raise Exception(f"direction should be 'upper' or 'below'. '{direction}' is not acceptable!")

    assert inactive_bases[f'internal_{high_low}'].notna().all()
    if f'{direction}_band_activated' in inactive_bases.columns:
        assert inactive_bases[f'{direction}_band_activated'].isna().all()
    else:
        inactive_bases[f'{direction}_band_activated'] = pd.Series(dtype='datetime64[ns, UTC]')

    for start, base in inactive_bases.iterrows():
        inactive_bases.loc[start, f'{direction}_band_activated'] = \
            first_passed_candle(start, base['ttl'], expand(base[f'internal_{high_low}'], base['ATR']), ohlcva,
                                timeframe, direction)
    return inactive_bases


def timeframe_effective_bases(_multi_timeframe_base_pattern, timeframe):
    try:
        index = config.timeframes.index(timeframe)
    except ValueError:
        raise Exception(f'timeframe:{timeframe} should be in [{config.timeframes}]!')
    except Exception as e:
        raise e
    if index < (len(config.timeframes) - 1):
        result = _multi_timeframe_base_pattern.loc[
            _multi_timeframe_base_pattern.index.get_level_values('timeframe') \
                .isin(config.timeframes[:index + 1])]
        return result
    else:
        return _multi_timeframe_base_pattern
