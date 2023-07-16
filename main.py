import pandas as pd

from BullBearSide import  multi_timeframe_trend_boundaries, plot_bull_bear_side, \
    read_multi_timeframe_trend_boundaries
from Config import TopTYPE, config
from DataPreparation import read_ohlca, read_multi_timeframe_ohlca
from PeaksValleys import merge_tops, read_peaks_n_valleys

if __name__ == "__main__":
    # ohlca = read_multi_timeframe_ohlca(config.under_process_date_range)
    # peaks_n_valleys = read_peaks_n_valleys(config.under_process_date_range)
    # candle_trend = candles_trend_multi_timeframe(ohlca, peaks_n_valleys)
    boundaries = read_multi_timeframe_trend_boundaries(config.under_process_date_range)

