import pandas as pd

from BullBearSide import candles_trend_multi_timeframe, trend_boundaries, plot_bull_bear_side
from Config import TopTYPE, config
from DataPreparation import read_ohlca, read_multi_timeframe_ohlca
from PeaksValleys import merge_tops, read_peaks_n_valleys

if __name__ == "__main__":
    ohlca = read_multi_timeframe_ohlca(config.under_process_date_range)
    peaks_n_valleys = read_peaks_n_valleys(config.under_process_date_range)
    candle_trend = candles_trend_multi_timeframe(ohlca, peaks_n_valleys)
    boundaries = trend_boundaries(ohlca)
    plot_bull_bear_side(ohlca, peaks_n_valleys, boundaries,
                        html_path=f'bull_bear_side.{config.under_process_date_range}.html')

