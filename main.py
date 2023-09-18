import os
from sys import exit

from Candle import generate_ohlc
from Config import config
from MetaTrader import MT
from PeakValley import generate_multi_timeframe_peaks_n_valleys
from fetch_ohlcv import under_process_date_range

if __name__ == "__main__":
    config.under_process_date_range = under_process_date_range(days=60)
    generate_ohlc()
    # generate_multi_timeframe_peaks_n_valleys(config.under_process_date_range)
    date_range_str = config.under_process_date_range
    file_path = config.path_of_data
    MT.extract_to_data_path(os.path.join(file_path, f'multi_timeframe_peaks_n_valleys.{date_range_str}.zip'))
    # generate_multi_timeframe_candle_trend(config.under_process_date_range)
    # generate_multi_timeframe_bull_bear_side_trends(config.under_process_date_range, timeframe_shortlist=['4H'])
    # generate_multi_timeframe_bull_bear_side_pivots(config.under_process_date_range, timeframe_shortlist=['4H'])
    # generate_multi_timeframe_top_pivots(config.under_process_date_range)  # , timeframe_shortlist=['15min'])
    exit()
