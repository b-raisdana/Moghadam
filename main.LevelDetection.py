import pandas as pd

from FigurePlotters import plot_ohlc_with_peaks_n_valleys, plotfig
from LevelDetection import find_peaks_n_valleys
from LevelDetectionConfig import config
from test_LevelDetection import test_time_switching, even_distribution, test_every_peak_is_found, test_strength_of_peaks

if __name__ == "__main__":

    test_time_switching()
    test_every_peak_is_found()
    test_strength_of_peaks()
    exit(0)
    # os.system((f'wget {config.data_path_preamble}/{config.files_to_load[0]}.zip -O {config.files_to_load[0]}.zip'))

    reverse_prices = pd.read_csv(f'{config.files_to_load[0]}.zip', index_col='date', parse_dates=['date'])
    # print(reverse_prices)
    # plotfig(reverse_prices.head(10000), name='reverse_prices Head')
    # plotfig(reverse_prices.tail(10000), name='reverse_prices Tail')

    test_prices = reverse_prices.tail(1000)
    # print(test_prices)
    # plotfig(test_prices, name='test prices', save=False, do_not_show=True)

    peaks, valleys = level_extractor(test_prices)

    print('Peaks:\n', peaks.sort_values(by='strength', ascending=False))
    # plot_ohlc_with_peaks_n_valleys(ohlc=test_prices, name='Test Prices (Base = 1min)', peaks=peaks, valleys=valleys)

    # for i in range(len(config.times)):
    #     aggregate_test_prices = test_prices.groupby(pd.Grouper(freq=config.times[i])) \
    #         .agg({'open': 'first',
    #               'close': 'last',
    #               'low': 'min',
    #               'high': 'max',
    #               'volume': 'sum'})
    #     _peaks = peaks[peaks['effective_time'].isin(config.times[i:])]
    #     _valleys = valleys[valleys['effective_time'].isin(config.times[i:])]
    #
    #     plot_ohlc_with_peaks_n_valleys(ohlc=aggregate_test_prices, name=config.times[i], peaks=_peaks, valleys=_valleys)
    #
    #     _time_peaks, _time_valleys = level_extractor(aggregate_test_prices)
    #     plot_ohlc_with_peaks_n_valleys\
    #         (ohlc=aggregate_test_prices, name=f'B{config.times[i]}', peaks=_time_peaks, valleys=_time_valleys)

    # running test in main for debug
