import pandas as pd

from FigurePlotters import plot_ohlc_with_peaks_n_valleys
from LevelDetection import level_extractor
from LevelDetectionConfig import config
from plotfig import plotfig
from test_LevelDetection import test_time_switching, even_distribution

if __name__ == "__main__":

    # os.system((f'wget {config.data_path_preamble}/{config.files_to_load[0]}.zip -O {config.files_to_load[0]}.zip'))

    reverse_prices = pd.read_csv(f'{config.files_to_load[0]}.zip', index_col='date', parse_dates=['date'])
    # print(reverse_prices)
    # plotfig(reverse_prices.head(10000), name='reverse_prices Head')
    # plotfig(reverse_prices.tail(10000), name='reverse_prices Tail')

    test_prices = reverse_prices.tail(1000)
    # print(test_prices)
    plotfig(test_prices, name='test prices', save=False, do_not_show=True)

    peaks, valleys = level_extractor(test_prices)

    print('Peaks:\n', peaks.sort_values(by='strength', ascending=False))
    plot_ohlc_with_peaks_n_valleys(ohlc=test_prices, name='Test Prices (Base = 1min)', peaks=peaks, valleys=valleys)

    # todo: map strength to time to detect the most major time of S/Ps

    for i in range(len(config.times)):
        aggregate_test_prices = test_prices.groupby(pd.Grouper(freq=config.times[i])) \
            .agg({'open': 'first',
                  'close': 'last',
                  'low': 'min',
                  'high': 'max',
                  'volume': 'sum'})
        _peaks = peaks[peaks['effective_time'].isin(config.times[i:])]
        _valleys = valleys[valleys['effective_time'].isin(config.times[i:])]

        plot_ohlc_with_peaks_n_valleys(ohlc=aggregate_test_prices, name=config.times[i], peaks=_peaks, valleys=_valleys)

    # running test in main for debug
    even_distribution(peaks, valleys)
    test_time_switching()
