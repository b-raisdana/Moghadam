import math

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
    # for i in range(len(peaks)):
    #     if i > 0:
    #         number_of_valleys_between_last_2_peaks = joined[
    #             peaks.index.values[i - 1] <= joined.index <= peaks.index.values[i]]
    #         assert number_of_valleys_between_last_2_peaks == 1
    #     if i < len(peaks) - 1:
    #         umber_of_valleys_between_next_2_peaks = joined[
    #             peaks.index.values[i] <= joined.index <= peaks.index.values[i + 1]]
    #         assert number_of_valleys_between_last_2_peaks == 1


base_ohlc_ticks = pd.read_csv(f'{config.files_to_load[0]}.zip', index_col='date', parse_dates=['date'], nrows=100)
if DEBUG: print(base_ohlc_ticks)
base_peaks, base_valleys = level_extractor(base_ohlc_ticks)


def test_time_switching():
    # try to check if every

    # todo: check index mapping after time switching

    for i in range(1, len(config.times)):
        _time_ohlc_ticks = base_ohlc_ticks.groupby(pd.Grouper(freq=config.times[i])) \
            .agg({'open': 'first',
                  'close': 'last',
                  'low': 'min',
                  'high': 'max',
                  'volume': 'sum'})

        _time_peaks, _time_valleys = level_extractor(_time_ohlc_ticks)
        _mapped_peaks_from_base = base_peaks[base_peaks['effective_time'].isin(config.times[i:])]

        comparable_time_peaks = _time_peaks[['high', 'effective_time']]
        comparable_mapped_base_peaks = _mapped_peaks_from_base[['high', 'effective_time']]
        for j in comparable_time_peaks.index.values:
            time_aligned_base_peaks = comparable_mapped_base_peaks[j:j + pd.to_timedelta(config.times[i])]
            _t = comparable_time_peaks[j]
            try:
                assert comparable_time_peaks[j] in time_aligned_base_peaks
                # assert _time_peaks.values == base_peaks[base_peaks['effective_time'].isin(config.times[i:])].values
            except Exception as e:
                plot_ohlc_with_peaks_n_valleys(ohlc=base_ohlc_ticks, name=f'base_ohlc_ticks  {config.times[i]}',
                                               peaks=_mapped_peaks_from_base)
                plot_ohlc_with_peaks_n_valleys(ohlc=_time_ohlc_ticks, name=f'_time_ohlc_ticks {config.times[i]}',
                                               peaks=_time_peaks, valleys=_time_valleys)
                # todo: ValueError: Can only compare identically-labeled DataFrame objects
                print(f"comparable_time_peaks({comparable_time_peaks.columns}):")
                print(comparable_time_peaks)
                print(f"comparable_mapped_base_peaks({comparable_mapped_base_peaks.columns}):")
                print(comparable_mapped_base_peaks)
                raise e

        try:
            assert _time_valleys == base_valleys[base_valleys['effective_time'].isin(config.times[i:])]
        except Exception as e:
            print(f"_time_valleys:{_time_valleys.info}")
            print(_time_valleys)
            print("base_valleys[base_valleys['effective_time'].isin(config.times[i:])]:")
            print(base_valleys[base_valleys['effective_time'].isin(config.times[i:])])
            raise e


def test_every_peak_is_found():
    for i in range(1, len(base_ohlc_ticks) - 1):
        if base_ohlc_ticks.iloc[i - 1]['high'] < \
                base_ohlc_ticks.iloc[i]['high'] > \
                base_ohlc_ticks.iloc[i + 1]['high']:
            try:
                assert base_peaks[base_ohlc_ticks.index[i]]['high'] == base_ohlc_ticks.iloc[i]['high']
            except Exception as e:
                pass
                print("base_peaks[base_ohlc_ticks.index[i]]['high']")
                print(base_peaks[base_ohlc_ticks.index[i]]['high'])
                print("base_ohlc_ticks.iloc[i]['high']")
                print(base_ohlc_ticks.iloc[i]['high'])
                raise e


def test_strength_of_peaks():
    for _t_index in base_peaks.index.values:
        right_candles = base_ohlc_ticks[_t_index < base_peaks.index <
                                        (_t_index + base_peaks.iloc[_t_index]['strength'])]
        assert base_peaks[_t_index]['high'] > right_candles.max()['high']

        left_candles = base_ohlc_ticks[_t_index - base_peaks.iloc[_t_index]['strength']
                                       < base_peaks.index < _t_index]
        assert base_peaks[_t_index]['high'] > left_candles.max()['high']

        if _t_index + base_peaks[_t_index]['strength'] < base_peaks.index[-1]:
            if _t_index - base_peaks.iloc[_t_index]['strength'] > base_peaks.index[0]:
                # we are the middle of series
                assert base_peaks[_t_index + base_peaks['strength']]['high'] > base_peaks[_t_index]['high'] \
                       or base_peaks[_t_index - base_peaks['strength']]['high'] > base_peaks[_t_index]['high']
            else:
                # we are at the beginning of series
                assert base_peaks[_t_index]['strength'] == _t_index - base_peaks.index[0]
        else:
            # we are at the end of series
            assert base_peaks[_t_index - base_peaks['strength']]['high'] > base_peaks[_t_index]['high']

# todo: repeat above for valleys

# def test_strength_of_valleys():
#     for _t_index in base_valleys.index.values:
#         print(_t_index + base_valleys[_t_index]['strength'])
#         right_candles = base_ohlc_ticks[_t_index < base_valleys.index <
#                                         (_t_index + base_valleys[_t_index]['strength'])]
#         assert base_valleys[_t_index]['low'] < right_candles.min()['low']
#
#         left_candles = base_ohlc_ticks[_t_index - base_valleys[_t_index]['strength']
#                                        < base_valleys.index < _t_index]
#         assert base_valleys[_t_index]['low'] < left_candles.min()['low']
#
#         if _t_index + base_valleys[_t_index]['strength'] < base_valleys.index[-1]:
#             if _t_index - base_valleys[_t_index]['strength'] > base_valleys.index[0]:
#                 # we are the middle of series
#                 assert base_valleys[_t_index + base_valleys['strength']]['low'] < base_valleys[_t_index]['low'] \
#                        or base_valleys[_t_index - base_valleys['strength']]['low'] < base_valleys[_t_index]['low']
#             else:
#                 # we are at the beginning of series
#                 assert base_valleys[_t_index]['strength'] == _t_index - base_valleys.index[0]
#         else:
#             # we are at the end of series
#             assert base_valleys[_t_index - base_valleys['strength']]['low'] < base_valleys[_t_index]['low']
#
#
# def test_every_valley_is_found():
#     for i in range(1, len(base_ohlc_ticks) - 1):
#         if base_ohlc_ticks.iloc[i - 1]['low'] > \
#                 base_ohlc_ticks.iloc[i]['low'] < \
#                 base_ohlc_ticks.iloc[i + 1]['low']:
#             assert base_peaks.iloc[i]['low'] == base_ohlc_ticks.iloc[i]['low']
