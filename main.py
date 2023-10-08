import os
from sys import exit

import helper
from BullBearSidePivot import generate_multi_timeframe_bull_bear_side_pivots
from Candle import generate_ohlc
from PeakValleyPivots import generate_multi_timeframe_top_pivots
from Config import config
from MetaTrader import MT
from PeakValley import generate_multi_timeframe_peaks_n_valleys, read_multi_timeframe_peaks_n_valleys
from helper import date_range_to_string

if __name__ == "__main__":
    config.under_process_date_range = date_range_to_string(days=60)
    # generatePeaks_n_Valleys
    # _ohlc()
    # generate_multi_timeframe_peaks_n_valleys(config.under_process_date_range)
    # generate_multi_timeframe_candle_trend(config.under_process_date_range)
    # generate_multi_timeframe_bull_bear_side_trends(config.under_process_date_range, timeframe_shortlist=['4H'])
    # generate_multi_timeframe_bull_bear_side_pivots(config.under_process_date_range, timeframe_shortlist=['4H'])
    generate_multi_timeframe_top_pivots(config.under_process_date_range)  # , timeframe_shortlist=['15min'])


    exit()
