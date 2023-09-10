from ctypes import Union

import pandas as pd
from pandera import typing as pt

from Config import TopTYPE, config
from DataPreparation import to_timeframe
from Model.Pivot import Pivot
from PeakValley import peaks_only, valleys_only


def pivots_level_n_margins(single_timeframe_pivot_peaks_or_valleys: pd.DataFrame,
                           single_timeframe_pivots: pd.DataFrame,
                           timeframe: str,
                           candle_body_source: pd.DataFrame,
                           atr_source: pd.DataFrame) -> pd.DataFrame:
    """
    Calculate pivot levels and margins based on peak or valley type for a single timeframe.

    Args:
        single_timeframe_pivot_peaks_or_valleys (pd.DataFrame): DataFrame containing peak or valley data.
        single_timeframe_pivots (pd.DataFrame): DataFrame to store the processed pivot data.
        timeframe (str): The desired timeframe for mapping pivot times.
        candle_body_source (pd.DataFrame): DataFrame containing candle body data with 'open' and 'close' columns.
        atr_source (pd.DataFrame): DataFrame containing ATR (Average True Range) data with 'ATR' column.

    Returns:
        pd.DataFrame: Updated DataFrame with calculated pivot levels and margins.
    """
    if len(single_timeframe_pivot_peaks_or_valleys) != len(single_timeframe_pivots):
        raise Exception(f'single_timeframe_pivot_peaks_or_valleys({len(single_timeframe_pivot_peaks_or_valleys)}) '
                        f'and single_timeframe_pivots({len(single_timeframe_pivots)}) should have the same length')
    pivot_peaks = peaks_only(single_timeframe_pivot_peaks_or_valleys)
    single_timeframe_pivots = peaks_or_valleys_pivots_level_n_margins(pivot_peaks, TopTYPE.PEAK,
                                                                      single_timeframe_pivots, timeframe,
                                                                      candle_body_source, atr_source)
    pivot_valleys = valleys_only(single_timeframe_pivot_peaks_or_valleys)
    single_timeframe_pivots = peaks_or_valleys_pivots_level_n_margins(pivot_valleys, TopTYPE.VALLEY,
                                                                      single_timeframe_pivots, timeframe,
                                                                      candle_body_source, atr_source)
    return single_timeframe_pivots


def peaks_or_valleys_pivots_level_n_margins(single_timeframe_pivot_peaks_or_valleys: pd.DataFrame,
                                            _type: TopTYPE,
                                            single_timeframe_pivots: pd.DataFrame,
                                            timeframe: str,
                                            candle_body_source: pd.DataFrame,
                                            atr_source: pd.DataFrame) -> pd.DataFrame:
    """
    Processes the pivot data to determine levels, margins, and other metrics for a single timeframe.

    Args:
        single_timeframe_pivot_peaks_or_valleys (pd.DataFrame): Input pivot data, typically containing high and low prices.
        _type (TopTYPE): Enum indicating whether the pivot data represents peaks or valleys.
        single_timeframe_pivots (pd.DataFrame): DataFrame to store processed pivot data.
        timeframe (str): A string specifying the desired timeframe for mapping pivot times.
        candle_body_source (pd.DataFrame): DataFrame containing 'open', 'high', 'low', 'close' columns for specific timeframes.
        atr_source (pd.DataFrame): DataFrame containing ATR (Average True Range) data with 'ATR' column.

    Returns:
        pd.DataFrame: Updated single_timeframe_pivots DataFrame with the processed pivot data.

    Raises:
        ValueError: If an invalid _type is provided or if the timeframe is not valid.
    """

    if _type.value not in [e.value for e in TopTYPE]:
        raise ValueError("Invalid type. Use either 'peak' or 'valley'.")

    if timeframe not in config.timeframes:
        raise ValueError(f"'{timeframe}' is not a valid timeframe. Please select from {config.timeframe}.")

    if len(single_timeframe_pivot_peaks_or_valleys) == 0:
        return single_timeframe_pivots

    if _type.value == 'peak':
        level_key = 'high'
    else:  # 'valley'
        level_key = 'low'

    pivot_times = single_timeframe_pivot_peaks_or_valleys.index.get_level_values('date')
    single_timeframe_pivots.loc[pivot_times, 'level'] = single_timeframe_pivot_peaks_or_valleys[level_key].tolist()

    single_timeframe_pivots = pivot_margins(single_timeframe_pivots, _type, single_timeframe_pivot_peaks_or_valleys,
                                            candle_body_source, timeframe, atr_source)

    return single_timeframe_pivots


def pivot_margins(pivots: pd.DataFrame, _type: TopTYPE, pivot_peaks_or_valleys: pd.DataFrame,
                  candle_body_source: pd.DataFrame, timeframe: str, atr_source: pd.DataFrame) -> pd.DataFrame:
    """
    Calculate margins for pivot levels based on peak or valley type.

    Args:
        pivots (pd.DataFrame): DataFrame containing pivot levels with a 'level' column.
        _type (TopTYPE): Type of pivot, either 'peak' or 'valley'.
        pivot_peaks_or_valleys (pd.DataFrame): DataFrame containing peak or valley information.
        candle_body_source (pd.DataFrame): DataFrame containing candle body data with 'open' and 'close' columns.
        timeframe (str): Timeframe used for mapping pivot times.
        atr_source (pd.DataFrame): DataFrame containing ATR (Average True Range) data with 'ATR' column.

    Returns:
        pd.DataFrame: Updated DataFrame with calculated margins.
    """
    if _type.value not in ['peak', 'valley']:
        raise ValueError("Invalid type. Use either 'peak' or 'valley'.")
    if _type.value == 'peak':
        choose_body_operator = max
        internal_func = min
    else:  # 'valley'
        choose_body_operator = min
        internal_func = max

    focused_pivots_times = pivot_peaks_or_valleys.index.get_level_values('date')
    focused_pivots = pivots.loc[focused_pivots_times]
    pivot_times_mapped_to_timeframe = [to_timeframe(pivot_time, timeframe) for pivot_time in focused_pivots_times]

    if _type.value == TopTYPE.PEAK.value:
        focused_pivots['nearest_body'] = \
            candle_body_source.loc[pivot_times_mapped_to_timeframe, ['open', 'close']] \
                .apply(choose_body_operator, axis='columns').tolist()
    else:
        focused_pivots['nearest_body'] = candle_body_source.loc[
            pivot_times_mapped_to_timeframe, ['open', 'close']] \
            .apply(choose_body_operator, axis='columns').tolist()

    pivots_atr = atr_source.loc[pivot_times_mapped_to_timeframe, 'ATR'].tolist()

    if _type.value == TopTYPE.PEAK.value:
        focused_pivots['ATR_margin'] = [level - atr for level, atr in
                                        zip(focused_pivots['level'].tolist(), pivots_atr)]
    else:
        focused_pivots['ATR_margin'] = focused_pivots['level'].add(pivots_atr).tolist()

    focused_pivots['internal_margin'] = focused_pivots[['nearest_body', 'ATR_margin']].apply(
        internal_func, axis='columns').tolist()

    if _type.value == TopTYPE.PEAK.value:
        focused_pivots['external_margin'] = focused_pivots['level'].add(pivots_atr).tolist()
    else:
        focused_pivots['external_margin'] = \
            [level - atr for level, atr in zip(pivots.loc[focused_pivots_times, 'level'].to_list(), pivots_atr)]
    pivots.loc[focused_pivots_times, ['internal_margin', 'external_margin']] = \
        focused_pivots[['internal_margin', 'external_margin']]
    return pivots
