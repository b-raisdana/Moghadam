import pandas as pd

from Config import config


def multi_timeframe_static_level_pivots(multi_timeframe_ohlca, multi_timeframe_peaks_n_valleys: pd.DataFrame):
    # definition of pivot:
    # highest high of every Bullish and lowest low of every Bearish trend.
    # todo: test timeframe of SR as:
    # timeframe of trend
    # timeframe of top
    for i, _timeframe in enumerate(config.structure_timeframes):
        trends =

def multi_timeframe_pivots(multi_timeframe_ohlca: pd.DataFrame, multi_timeframe_peaks_n_valleys: pd.DataFrame):
    #
    # _static_level_pivots.columns = ['date', 'death_on': 1st FTC or None if the
    # If level has been being used >> 'death_on': the time of passing
    # If level has been being passed >> 'death_on': None to reactivate static level
    _static_level_pivots = multi_timeframe_static_level_pivots(multi_timeframe_ohlca, multi_timeframe_peaks_n_valleys)

    # todo: add gap_level_pivots

