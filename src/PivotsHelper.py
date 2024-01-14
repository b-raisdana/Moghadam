import datetime

import pandas as pd

from Config import TopTYPE, config
from PeakValley import peaks_only, valleys_only
from helper.data_preparation import to_timeframe
from helper.helper import measure_time


@measure_time
def pivots_level_n_margins(pivot_peaks_n_valleys: pd.DataFrame,
                           timeframe_pivots: pd.DataFrame,
                           timeframe: str,
                           candle_body_source: pd.DataFrame,
                           internal_atr_source: pd.DataFrame,
                           breakout_atr_source: pd.DataFrame,
                           ) -> pd.DataFrame:
    """
    Calculate pivot levels and margins based on peak or valley type for a single timeframe.

    Args:
        pivot_peaks_n_valleys (pd.DataFrame): DataFrame containing peak or valley data.
        timeframe_pivots (pd.DataFrame): DataFrame to store the processed pivot data.
        timeframe (str): The desired timeframe for mapping pivot times.
        candle_body_source (pd.DataFrame): DataFrame containing candle body data with 'open' and 'close' columns.
        internal_atr_source (pd.DataFrame): DataFrame containing atr (Average True Range) data with 'atr' column.
        breakout_atr_source (pd.DataFrame): DataFrame containing atr (Average True Range) data with 'atr' column.

    Returns:
        pd.DataFrame: Updated DataFrame with calculated pivot levels and margins.
    """
    # if len(pivot_peaks_n_valleys) != len(timeframe_pivots):
    #     raise Exception(f'pivot_peaks_or_valleys({len(pivot_peaks_n_valleys)}) '
    #                     f'and timeframe_pivots({len(timeframe_pivots)}) should have the same length')
    if timeframe == '1h':
        pass
    no_timeframe_peaks_n_valleys = pivot_peaks_n_valleys.reset_index(level='timeframe')
    pivot_peaks = peaks_only(no_timeframe_peaks_n_valleys)
    timeframe_pivots = peaks_or_valleys_pivots_level_n_margins(pivot_peaks, TopTYPE.PEAK,
                                                               timeframe_pivots, timeframe,
                                                               candle_body_source, internal_atr_source,
                                                               breakout_atr_source)
    pivot_valleys = valleys_only(no_timeframe_peaks_n_valleys)
    timeframe_pivots = peaks_or_valleys_pivots_level_n_margins(pivot_valleys, TopTYPE.VALLEY,
                                                               timeframe_pivots, timeframe,
                                                               candle_body_source, internal_atr_source,
                                                               breakout_atr_source)
    return timeframe_pivots


def peaks_or_valleys_pivots_level_n_margins(no_timeframe_pivot_peaks_or_valleys: pd.DataFrame,
                                            _type: TopTYPE,
                                            timeframe_pivots: pd.DataFrame,
                                            timeframe: str,
                                            candle_body_source: pd.DataFrame,
                                            internal_margin_atr: pd.DataFrame,
                                            breakout_margin_atr: pd.DataFrame,
                                            ) -> pd.DataFrame:
    """
    Processes the pivot data to determine levels, margins, and other metrics for a single timeframe.

    Args:
        no_timeframe_pivot_peaks_or_valleys (pd.DataFrame): Input pivot data, typically containing high and low prices.
        _type (TopTYPE): Enum indicating whether the pivot data represents peaks or valleys.
        timeframe_pivots (pd.DataFrame): DataFrame to store processed pivot data.
        timeframe (str): A string specifying the desired timeframe for mapping pivot times.
        candle_body_source (pd.DataFrame): DataFrame containing 'open', 'high', 'low', 'close' columns for specific timeframes.
        internal_margin_atr (pd.DataFrame): DataFrame containing atr (Average True Range) data with 'atr' column.
        breakout_margin_atr (pd.DataFrame): DataFrame containing atr (Average True Range) data with 'atr' column.

    Returns:
        pd.DataFrame: Updated single_timeframe_pivots DataFrame with the processed pivot data.

    Raises:
        ValueError: If an invalid _type is provided or if the timeframe is not valid.
    """

    if _type.value not in [e.value for e in TopTYPE]:
        raise ValueError("Invalid type. Use either 'peak' or 'valley'.")
    if timeframe not in config.timeframes:
        raise ValueError(f"'{timeframe}' is not a valid timeframe. Please select from {config.timeframe}.")
    if len(no_timeframe_pivot_peaks_or_valleys) == 0:
        return timeframe_pivots

    if _type.value == 'peak':
        high_low = 'high'
    else:  # 'valley'
        high_low = 'low'
    no_timeframe_pivot_peaks_or_valleys['level_y'] = no_timeframe_pivot_peaks_or_valleys[[high_low]]
    timeframe_pivots = timeframe_pivots.merge(no_timeframe_pivot_peaks_or_valleys[['level_y']], how='left',
                                              left_index=True, right_index=True)
    timeframe_pivots.loc[no_timeframe_pivot_peaks_or_valleys.index, 'level'] = \
        timeframe_pivots.loc[no_timeframe_pivot_peaks_or_valleys.index, 'level_y']
    timeframe_pivots = timeframe_pivots.drop('level_y', axis='columns')
    if 'level' not in timeframe_pivots.columns:
        AssertionError("'level' not in timeframe_pivots.columns")
    if timeframe_pivots[['level']].isna().any().any():
        AssertionError("timeframe_pivots[['level']].isna().any().any()")
    timeframe_pivots = pivot_margins(timeframe_pivots, _type, no_timeframe_pivot_peaks_or_valleys,
                                     candle_body_source, timeframe, internal_margin_atr, breakout_margin_atr)
    return timeframe_pivots


@measure_time
def pivot_margins(pivots: pd.DataFrame, _type: TopTYPE, pivot_peaks_or_valleys: pd.DataFrame,
                  candle_body_source: pd.DataFrame, timeframe: str, internal_margin_atr: pd.DataFrame,
                  breakout_margin_atr: pd.DataFrame) -> pd.DataFrame:
    """
    Calculate margins for pivot levels based on peak or valley type.

    Args:
        pivots (pd.DataFrame): DataFrame containing pivot levels with a 'level' column.
        _type (TopTYPE): Type of pivot, either 'peak' or 'valley'.
        pivot_peaks_or_valleys (pd.DataFrame): DataFrame containing peak or valley information.
        candle_body_source (pd.DataFrame): DataFrame containing candle body data with 'open' and 'close' columns.
        timeframe (str): Timeframe used for mapping pivot times.
        internal_margin_atr (pd.DataFrame): DataFrame containing atr (Average True Range) data with 'atr' column.
        breakout_margin_atr (pd.DataFrame): DataFrame containing atr (Average True Range) data with 'atr' column.

    Returns:
        pd.DataFrame: Updated DataFrame with calculated margins.
    """
    if _type == TopTYPE.PEAK:
        choose_body_operator = max
        internal_func = min
    else:  # _type == TopTYPE.valley
        choose_body_operator = min
        internal_func = max
    pivot_times = pivot_peaks_or_valleys.index.get_level_values('date')
    # nearest_body = max/min(candle_body_source[['open', 'close']])
    candle_body_source['nearest_body'] = candle_body_source[['open', 'close']] \
        .apply(choose_body_operator, axis='columns')
    mapped_pivot_times = [to_timeframe(pivot_time, timeframe) for pivot_time in
                          pivot_peaks_or_valleys.index.get_level_values('date')]
    candle_body_source: pd.DataFrame = candle_body_source.loc[mapped_pivot_times, 'nearest_body']
    pivots = pd.merge_asof(pivots, candle_body_source, left_index=True, right_index=True, direction='backward',
                           suffixes=('_x', ''))
    # internal_margin = min/max( level -/+ internal_margin_atr ) of mapped time
    internal_margin_atr = internal_margin_atr.rename(columns={'atr': 'internal_margin_atr'})[['internal_margin_atr']]
    pivots = pd.merge_asof(pivots, internal_margin_atr, left_index=True, right_index=True, direction='backward',
                           suffixes=('_x', ''))
    if _type.value == TopTYPE.PEAK.value:
        pivots.loc[pivot_times, 'atr_margin'] = \
            pivots.loc[pivot_times, 'level'] - pivots.loc[pivot_times, 'internal_margin_atr']
    else:
        pivots.loc[pivot_times, 'atr_margin'] = \
            pivots.loc[pivot_times, 'level'] + pivots.loc[pivot_times, 'internal_margin_atr']
    pivots.loc[pivot_times, 'internal_margin'] = \
        pivots.loc[pivot_times, ['nearest_body', 'atr_margin']].apply(internal_func, axis='columns')
    # external_margin = level +/- breakout_margin_atr of mapped time
    breakout_margin_atr = breakout_margin_atr.rename(columns={'atr': 'breakout_margin_atr'})[['breakout_margin_atr']]
    pivots = pd.merge_asof(pivots, breakout_margin_atr, left_index=True, right_index=True, direction='backward',
                           suffixes=('_x', ''))
    if _type.value == TopTYPE.PEAK.value:
        pivots.loc[pivot_times, 'external_margin'] = \
            pivots.loc[pivot_times, 'level'] + pivots.loc[pivot_times, 'breakout_margin_atr']
    else:
        pivots.loc[pivot_times, 'external_margin'] = \
            pivots.loc[pivot_times, 'level'] - pivots.loc[pivot_times, 'breakout_margin_atr']
    if pivots[['internal_margin', 'external_margin']].isna().any().any():
        AssertionError("pivots[['internal_margin', 'external_margin']].isna().any().any()")
    return pivots


def level_ttl(timeframe) -> datetime.timedelta:
    return 512 * pd.to_timedelta(timeframe)
