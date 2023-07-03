import pandas as pd


def multi_time_static_level_pivots(multi_time_ohlca, multi_time_peaks_n_valleys: pd.DataFrame)
def multi_time_pivots(multi_time_ohlca: pd.DataFrame, multi_time_peaks_n_valleys: pd.DataFrame):
    #
    # _static_level_pivots.columns = ['date', 'death_on': 1st FTC or None if the
    # If level has been being used >> 'death_on': the time of passing
    # If level has been being passed >> 'death_on': None to reactivate static level
    _static_level_pivots = multi_time_static_level_pivots(multi_time_ohlca, multi_time_peaks_n_valleys)

    # todo: add gap_level_pivots

