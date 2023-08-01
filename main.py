import pandas as pd

from BullBearSide import multi_timeframe_trend_boundaries, plot_single_timeframe_trend_boundaries, \
    read_multi_timeframe_trend_boundaries, generate_multi_timeframe_trend_boundaries
from Config import TopTYPE, config

if __name__ == "__main__":
    # ohlca = read_multi_timeframe_ohlca(config.under_process_date_range)
    # peaks_n_valleys = read_peaks_n_valleys(config.under_process_date_range)
    # candle_trend = candles_trend_multi_timeframe(ohlca, peaks_n_valleys)
    generate_multi_timeframe_trend_boundaries(config.under_process_date_range)
    boundaries = read_multi_timeframe_trend_boundaries(config.under_process_date_range)


