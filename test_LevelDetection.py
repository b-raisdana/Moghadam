from FigurePlotters import plot_ohlc_with_peaks_n_valleys
import pandas as pd
from LevelDetectionConfig import INFINITY_TIME_DELTA, config
from LevelDetection import level_extractor

DEBUG = True


def even_distribution(peaks: pd, valleys: pd):
    # I found the peaks and valleys are not required to be distributed evenly\
    return True
    # peaks.insert(len(peaks.columns), 'is_a_peak', True)
    # valleys.insert(len(valleys.columns), 'is_a_valleys', True)
    #
    # joined = pd.merge(peaks, valleys)
    #
    # print(joined)
    #
    # for i in range(len(peaks.index.values)):
    #     if i > 0:
    #         number_of_valleys_between_last_2_peaks = joined[
    #             peaks.index.values[i - 1] <= joined.index <= peaks.index.values[i]]
    #         assert number_of_valleys_between_last_2_peaks == 1
    #     if i < len(peaks.index.values) - 1:
    #         umber_of_valleys_between_next_2_peaks = joined[
    #             peaks.index.values[i] <= joined.index <= peaks.index.values[i + 1]]
    #         assert number_of_valleys_between_last_2_peaks == 1


base_ohlc_ticks = pd.read_csv(f'{config.files_to_load[0]}.zip', index_col='date', parse_dates=['date'])
if DEBUG: print(base_ohlc_ticks)
base_peaks, _base_valleys = level_extractor(base_ohlc_ticks.tail(1000))


def test_time_switching():
    # try to check if every

    # todo: check index mapping after time switching

    for i in range(len(config.times)):
        _time_ohlc_ticks = base_ohlc_ticks.groupby(pd.Grouper(freq=config.times[i])) \
            .agg({'open': 'first',
                  'close': 'last',
                  'low': 'min',
                  'high': 'max',
                  'volume': 'sum'})

    _time_peaks, _time_valleys = level_extractor(_time_ohlc_ticks)
    _mapped_peaks_from_base = base_peaks['effective_time'].isin(config.times[i:])

    try:
        assert _time_peaks == base_peaks[base_peaks['effective_time'].isin(config.times[i:])]
    except AssertionError as e:
        plot_ohlc_with_peaks_n_valleys(ohlc=_time_ohlc_ticks, name=f'Test {config.times[i]}',
                                       peaks=_time_peaks, valleys=_time_valleys)

    try:
        assert _time_valleys == _base_valleys[_base_valleys['effective_time'].isin(config.times[i:])]
    except AssertionError as e:
        plot_ohlc_with_peaks_n_valleys(ohlc=_time_ohlc_ticks, name=f'Test {config.times[i]}',
                                       peaks=_time_peaks, valleys=_time_valleys)

    # even_distribution(_time_peaks, _time_valleys)


def test_every_peak_is_found():
    for i in range(1, len(base_ohlc_ticks) - 1):
        if base_ohlc_ticks.iloc[base_ohlc_ticks.index[i - 1]]['high'] < \
                base_ohlc_ticks.iloc[base_ohlc_ticks.index[i]]['high'] > \
                base_ohlc_ticks.iloc[base_ohlc_ticks.index[i + 1]]['high']:
            assert base_peaks.iloc[base_ohlc_ticks.index[i]]['high'] == base_ohlc_ticks.iloc[i]['high']

def test_strenght_of_peaks():
    for i in range(len(base_peaks)):
        right_candles = base_ohlc_ticks.iloc[base_peaks.index[i] < base_peaks.index < (base_peaks.index[i] + base_peaks.iloc[i]['strength'])]
        left_candles = base_ohlc_ticks.iloc[base_peaks.index[i] - base_peaks.iloc[i]['strength'] < base_peaks.index < (base_peaks.index[i] )]
        assert base_peaks.iloc[i]['high'] > right_candles.max()['high']
        if base_peaks.index[i] + base_peaks.iloc[i]['strength'] < base_peaks.index[-1]:

            if base_peaks.index[i] - base_peaks.iloc[i]['strength'] > base_peaks.index[0]:

            else:

        else:

        try:
        right_candle_at_strength = base_ohlc_ticks.iloc[base_peaks.index[i] + base_peaks.iloc[i]['strength']]
        left_candle_at_strength = base_ohlc_ticks.iloc[base_peaks.index[i] - base_peaks.iloc[i]['strength']]
        try:

        assert base_peaks.iloc[i]['high'] <= right_candles.max()['high']


def test_every_valley_is_found():
    for i in range(1, len(base_ohlc_ticks) - 1):
        if base_ohlc_ticks.iloc[base_ohlc_ticks.index[i - 1]]['low'] > \
                base_ohlc_ticks.iloc[base_ohlc_ticks.index[i]]['low'] < \
                base_ohlc_ticks.iloc[base_ohlc_ticks.index[i + 1]]['low']:
            assert base_peaks.iloc[base_ohlc_ticks.index[i]]['low'] == base_ohlc_ticks.iloc[i]['low']
