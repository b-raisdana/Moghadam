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

plot_ohlc_with_peaks_n_valleys(ohlc=base_ohlc_ticks, name=f'base_ohlc_ticks',
                               peaks=base_peaks, valleys=base_valleys)


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
        comparable_time_peaks = _time_peaks[['high', 'effective_time']] \
            .set_index(['high', 'effective_time'])
        comparable_mapped_base_peaks = _mapped_peaks_from_base[['high', 'effective_time']] \
            .set_index(['high', 'effective_time'])
        try:
            assert len(comparable_time_peaks.index.isin(comparable_mapped_base_peaks.index)) == \
                   len(comparable_time_peaks)
            # assert _time_peaks.values == base_peaks[base_peaks['effective_time'].isin(config.times[i:])].values
        except Exception as e:
            # plot_ohlc_with_peaks_n_valleys(ohlc=base_ohlc_ticks, name=f'base_ohlc_ticks  {config.times[i]}',
            #                                peaks=_mapped_peaks_from_base)
            # plot_ohlc_with_peaks_n_valleys(ohlc=_time_ohlc_ticks, name=f'_time_ohlc_ticks {config.times[i]}',
            #                                peaks=_time_peaks, valleys=_time_valleys)
            print(f"comparable_time_peaks({comparable_time_peaks.columns}):")
            print(comparable_time_peaks)
            print(f"comparable_mapped_base_peaks({comparable_mapped_base_peaks.columns}):")
            print(comparable_mapped_base_peaks)
            raise e

        _mapped_valleys_from_base = base_valleys[base_valleys['effective_time'].isin(config.times[i:])]
        comparable_time_valleys = _time_valleys[['low', 'effective_time']] \
            .set_index(['low', 'effective_time'])
        comparable_mapped_base_valleys = _mapped_valleys_from_base[['low', 'effective_time']] \
            .set_index(['low', 'effective_time'])
        try:
            assert len(comparable_time_valleys.index.isin(comparable_mapped_base_valleys.index)) == \
                   len(comparable_time_valleys)
        except Exception as e:
            plot_ohlc_with_peaks_n_valleys(ohlc=base_ohlc_ticks, name=f'base_ohlc_ticks  {config.times[i]}',
                                           valleys=_mapped_valleys_from_base)
            plot_ohlc_with_peaks_n_valleys(ohlc=_time_ohlc_ticks, name=f'_time_ohlc_ticks {config.times[i]}',
                                           peaks=_time_peaks, valleys=_time_valleys)
            # todo: ValueError: Can only compare identically-labeled DataFrame objects
            print(f"comparable_time_valleys({comparable_time_valleys.columns}):")
            print(comparable_time_valleys)
            print(f"comparable_mapped_base_valleys({comparable_mapped_base_valleys.columns}):")
            print(comparable_mapped_base_valleys)
            raise e


def test_every_peak_is_found():
    for i in range(1, len(base_ohlc_ticks) - 1):
        if base_ohlc_ticks.iloc[i - 1]['high'] < \
                base_ohlc_ticks.iloc[i]['high'] > \
                base_ohlc_ticks.iloc[i + 1]['high']:
            try:
                assert base_peaks.loc[base_ohlc_ticks.index[i]]['high'] == base_ohlc_ticks.iloc[i]['high']
            except Exception as e:
                pass
                print("base_peaks[base_ohlc_ticks.index[i]]['high']")
                print(base_peaks[base_ohlc_ticks.index[i]]['high'])
                print("base_ohlc_ticks.iloc[i]['high']")
                print(base_ohlc_ticks.iloc[i]['high'])
                raise e


def test_strength_of_peaks():
    for _t_index in base_peaks.index:
        right_candles = base_ohlc_ticks[(_t_index < base_ohlc_ticks.index) &
                                        (base_ohlc_ticks.index < (_t_index + base_peaks.loc[_t_index]['strength']))]
        assert base_peaks.loc[_t_index]['high'] >= right_candles.max()['high']

        left_candles = base_ohlc_ticks[(_t_index - base_peaks.loc[_t_index]['strength'] < base_ohlc_ticks.index) &
                                       (base_ohlc_ticks.index < _t_index)]
        assert base_peaks.loc[_t_index]['high'] >= left_candles.max()['high']

        if _t_index - base_peaks.loc[_t_index]['strength'] > base_peaks.index[0]:
            if _t_index + base_peaks.loc[_t_index]['strength'] < base_peaks.index[-1]:
                # the peak is not top in either sides
                assert base_ohlc_ticks.loc[_t_index + base_peaks.loc[_t_index]['strength']]['high'] > base_peaks.loc[_t_index]['high'] \
                       or base_ohlc_ticks.loc[_t_index - base_peaks.loc[_t_index]['strength']]['high'] > base_peaks.loc[_t_index]['high']
            else:
                # the peak is top in all next candles
                assert base_ohlc_ticks.loc[_t_index - base_peaks.loc[_t_index]['strength']]['high'] > \
                       base_peaks.loc[_t_index]['high']
        else:
            # the peak is top in all before candles
            assert base_peaks.loc[_t_index]['strength'] == _t_index - base_ohlc_ticks.index[0]

# todo: repeat above for valleys
