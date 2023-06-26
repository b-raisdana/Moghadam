import pandas as pd

from BullBearSide import bull_bear_side, trend_boundaries, plot_bull_bear_side
from Config import config, TopTYPE
from PeaksValleys import generate_test_ohlc, generate_peaks_n_valleys_csv, merge_tops
from test_PeaksValleys import test_time_switching, even_distribution, test_every_peak_is_found, test_strength_of_peaks

if __name__ == "__main__":
    # generate_test_ohlc()
    # generate_peaks_n_valleys_CSVs()
    # os.system((f'wget {config.data_path_preamble}/{config.files_to_load[0]}.zip -O {config.files_to_load[0]}.zip'))

    prices = pd.read_csv(f'ohlc.17-10-06.00-00T17-10-06.23-59.zip', sep=',', header=0, index_col='date',
                         parse_dates=['date'])

    peaks = pd.read_csv(f'peaks.17-10-06.00-00T17-10-06.23-59.zip', index_col='date', parse_dates=['date'], header=0)
    valleys = pd.read_csv(f'valleys.17-10-06.00-00T17-10-06.23-59.zip', index_col='date', parse_dates=['date'],
                          header=0)
    peaks['peak_or_valley'] = TopTYPE.PEAK
    valleys['peak_or_valley'] = TopTYPE.VALLEY
    peaks_n_valleys = merge_tops(peaks, valleys)
    prices = bull_bear_side(prices, peaks_n_valleys)
    _boundaries = trend_boundaries(prices)
    plot_bull_bear_side(prices, peaks_n_valleys, _boundaries, html_path='test_plot.html')
    # test_bull_bear_side(test_prices, peaks, valleys)

# def test_peaks_valleys():
#     test_time_switching()
#     test_every_peak_is_found()
#     test_strength_of_peaks()
