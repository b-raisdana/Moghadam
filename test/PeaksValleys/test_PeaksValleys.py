import pandas as pd

from Config import config
from PeakValley import zz_find_peaks_n_valleys, plot_peaks_n_valleys

DEBUG = True


def even_distribution(peaks: pd, valleys: pd):
    # the peaks and valleys are not required to be distributed evenly\
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


# base_ohlc_ticks = pd.read_csv(f'{config.files_to_load[0]}.zip', sep=',', header=0, index_col='date',
#                               parse_dates=['date'], skiprows=range(1, 400320), nrows=1440)
# if DEBUG: print(base_ohlc_ticks)
# # base_ohlc_ticks['indexer'] = range(0,len(base_ohlc_ticks))
# base_peaks, base_valleys = find_peaks_n_valleys(base_ohlc_ticks)
# # base_peaks.to_csv(f'peaks.{base_ohlc_ticks.index[0].strftime("%y-%m-%d.%H-%M")}T{base_ohlc_ticks.index[-1].strftime("%y-%m-%d.%H-%M")}.zip')
# # base_valleys.to_csv(f'valleys.{base_ohlc_ticks.index[0].strftime("%y-%m-%d.%H-%M")}T{base_ohlc_ticks.index[-1].strftime("%y-%m-%d.%H-%M")}.zip')
#
# # plot_ohlc_with_peaks_n_valleys(ohlc=base_ohlc_ticks, name=f'base_ohlc_ticks',
# #                                peaks=base_peaks, valleys=base_valleys)


def test_timeframe_switching():
    for i in range(1, len(config.timeframes)):
        _timeframe_ohlc_ticks = base_ohlc_ticks.groupby(pd.Grouper(freq=config.timeframes[i])) \
            .agg({'open': 'first',
                  'close': 'last',
                  'low': 'min',
                  'high': 'max',
                  'volume': 'sum'})

        _timeframe_peaks, _timeframe_valleys = zz_find_peaks_n_valleys(_timeframe_ohlc_ticks)

        _mapped_peaks_from_base = base_peaks[base_peaks['timeframe'].isin(config.timeframes[i:])]
        # todo: test set_index(['high', 'timeframe']
        comparable_timeframe_peaks = _timeframe_peaks[['high', 'timeframe']] \
            .set_index(['high', 'timeframe'])
        comparable_mapped_base_peaks = _mapped_peaks_from_base[['high', 'timeframe']] \
            .set_index(['high', 'timeframe'])
        try:
            assert len(comparable_timeframe_peaks.index.isin(comparable_mapped_base_peaks.index)) == \
                   len(comparable_timeframe_peaks)
            # assert _timeframe_peaks.values == base_peaks[base_peaks['timeframe'].isin(config.times[i:])].values
        except Exception as e:
            # plot_ohlc_with_peaks_n_valleys(ohlc=base_ohlc_ticks, name=f'base_ohlc_ticks  {config.times[i]}',
            #                                peaks=_mapped_peaks_from_base)
            # plot_ohlc_with_peaks_n_valleys(ohlc=_timeframe_ohlc_ticks, name=f'_timeframe_ohlc_ticks {config.times[i]}',
            #                                peaks=_timeframe_peaks, valleys=_timeframe_valleys)
            log(f"comparable_timeframe_peaks({comparable_timeframe_peaks.columns}):")
            log(comparable_timeframe_peaks)
            log(f"comparable_mapped_base_peaks({comparable_mapped_base_peaks.columns}):")
            log(comparable_mapped_base_peaks)
            raise e

        _mapped_valleys_from_base = base_valleys[base_valleys['timeframe'].isin(config.timeframes[i:])]
        # todo: test set_index(['low', 'timeframe']
        comparable_timeframe_valleys = _timeframe_valleys[['low', 'timeframe']] \
            .set_index(['low', 'timeframe'])
        comparable_mapped_base_valleys = _mapped_valleys_from_base[['low', 'timeframe']] \
            .set_index(['low', 'timeframe'])
        try:
            assert len(comparable_timeframe_valleys.index.isin(comparable_mapped_base_valleys.index)) == \
                   len(comparable_timeframe_valleys)
        except Exception as e:
            # plot_peaks_n_valleys(ohlc=base_ohlc_ticks, name=f'base_ohlc_ticks  {config.timeframes[i]}',
            #                      valleys=_mapped_valleys_from_base)
            # plot_peaks_n_valleys(ohlc=_timeframe_ohlc_ticks, name=f'_timeframe_ohlc_ticks {config.timeframes[i]}',
            #                      peaks=_timeframe_peaks, valleys=_timeframe_valleys)
            log(f"comparable_timeframe_valleys({comparable_timeframe_valleys.columns}):")
            log(comparable_timeframe_valleys)
            log(f"comparable_mapped_base_valleys({comparable_mapped_base_valleys.columns}):")
            log(comparable_mapped_base_valleys)
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
                log("base_peaks[base_ohlc_ticks.index[i]]['high']")
                log(base_peaks[base_ohlc_ticks.index[i]]['high'])
                log("base_ohlc_ticks.iloc[i]['high']")
                log(base_ohlc_ticks.iloc[i]['high'])
                raise e


def test_strength_of_peaks():
    for _t_index in base_peaks.index:
        right_candles = base_ohlc_ticks[(_t_index < base_ohlc_ticks.index) &
                                        (base_ohlc_ticks.index < (_t_index + base_peaks.loc[_t_index]['strength']))]
        assert base_peaks.loc[_t_index]['high'] >= right_candles.max()['high']

        left_candles = base_ohlc_ticks[(_t_index - base_peaks.loc[_t_index]['strength'] < base_ohlc_ticks.index) &
                                       (base_ohlc_ticks.index < _t_index)]
        assert base_peaks.loc[_t_index]['high'] >= left_candles.max()['high']

        if _t_index - base_peaks.loc[_t_index]['strength'] > base_ohlc_ticks.index[0]:
            if _t_index + base_peaks.loc[_t_index]['strength'] < base_ohlc_ticks.index[-1]:
                # the peak is not top in either sides
                assert base_ohlc_ticks.loc[_t_index + base_peaks.loc[_t_index]['strength']]['high'] > \
                       base_peaks.loc[_t_index]['high'] \
                       or base_ohlc_ticks.loc[_t_index - base_peaks.loc[_t_index]['strength']]['high'] > \
                       base_peaks.loc[_t_index]['high']
            else:
                # the peak is top in all next candles
                assert base_ohlc_ticks.loc[_t_index - base_peaks.loc[_t_index]['strength']]['high'] > \
                       base_peaks.loc[_t_index]['high']
        else:
            # the peak is top in all before candles
            assert base_peaks.loc[_t_index]['strength'] == _t_index - base_ohlc_ticks.index[0]

        # todo: all asserts are reached
# todo: repeat above for valleys
