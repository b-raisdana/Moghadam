# import talib as ta
import datetime
from datetime import timedelta

import pandas as pd

from FigurePlotters import plot_ohlc_with_peaks_n_valleys, plotfig
from LevelDetectionConfig import config, INFINITY_TIME_DELTA

DEBUG = True


def level_extractor(prices: pd.DataFrame, min_weight=None, significance=None, max_cycles=100):
    _peaks = peaks_valleys_extractor(prices, True, min_weight, significance, max_cycles)
    _valleys = peaks_valleys_extractor(prices, False, min_weight, significance, max_cycles)
    return _peaks, _valleys


def peaks_valleys_extractor(prices: pd.DataFrame, peaks_mode: bool = True, min_strength: timedelta = None,
                            ignore_n_percent_lowest_strength=None, max_cycles=100):
    valleys_mode = not peaks_mode
    peaks_valleys: pd = prices.copy()  # pd.DataFrame(prices.index)
    peaks_valleys.insert(len(peaks_valleys.columns), 'strength', INFINITY_TIME_DELTA)
    if peaks_mode:
        peaks_valleys.insert(len(peaks_valleys.columns), 'next_high', peaks_valleys['high'].shift(-1))
        peaks_valleys.insert(len(peaks_valleys.columns), 'previous_high', peaks_valleys['high'].shift(1))
        peaks_valleys = peaks_valleys[peaks_valleys['previous_high'] <= peaks_valleys['high']]
        peaks_valleys = peaks_valleys.loc[peaks_valleys['high'] >= peaks_valleys['next_high']]
        peaks_valleys = peaks_valleys.drop(labels=['next_high', 'previous_high'], axis=1)
    else:  # valleys_mode
        peaks_valleys.insert(len(peaks_valleys.columns), 'next_low', peaks_valleys['low'].shift(-1))
        peaks_valleys.insert(len(peaks_valleys.columns), 'previous_low', peaks_valleys['low'].shift(1))
        peaks_valleys = peaks_valleys[peaks_valleys['previous_low'] >= peaks_valleys['low']]
        peaks_valleys = peaks_valleys[peaks_valleys['low'] <= peaks_valleys['next_low']]
        peaks_valleys = peaks_valleys.drop(labels=['next_low', 'previous_low'], axis=1)

    # if DEBUG: print(
    #     f"peaks_valleys[peaks_valleys['strength']==float('inf'):{peaks_valleys[peaks_valleys['strength'] == float('inf')]}")
    # if DEBUG: print(
    #     f"len(peaks_valleys[peaks_valleys['strength']==float('inf')].index.values){len(peaks_valleys[peaks_valleys['strength'] == float('inf')].index.values)}")
    # if DEBUG: print(
    #     f"peaks_valleys[peaks_valleys['strength']==float('inf')]:{peaks_valleys[peaks_valleys['strength'] == float('inf')]}")
    peaks_valleys = peaks_valleys[peaks_valleys['volume'] > 0]
    for i in range(1, len(peaks_valleys)):
        if valleys_mode:
            if peaks_valleys.iloc[i]['low'] == peaks_valleys.iloc[i - 1]['low']:
                peaks_valleys.at[peaks_valleys.index[i], 'strength'] = timedelta(0)
        else:  # peaks_mode
            if peaks_valleys.iloc[i]['high'] == peaks_valleys.iloc[i - 1]['high']:
                peaks_valleys.at[peaks_valleys.index[i], 'strength'] = timedelta(0)
    peaks_valleys = peaks_valleys[peaks_valleys['strength'] > timedelta(0)]
    for i in range(len(peaks_valleys.index.values)):
        if DEBUG and peaks_valleys.index[i] == datetime.datetime.strptime('12/31/2017 09:09', '%m/%d/%Y %H:%M'):
            pass
        left_distance = INFINITY_TIME_DELTA
        right_distance = INFINITY_TIME_DELTA

        if valleys_mode:
            if i > 0:
                left_lower_valleys = prices[prices.index < peaks_valleys.index[i]].loc[prices['low'] <
                                                                                       peaks_valleys.iloc[i]['low']]
                if len(left_lower_valleys.index.values) > 0:
                    left_distance = (peaks_valleys.index[i] - left_lower_valleys.index[-1])
                    # check if at least one higher valley exist in the range
                    higher_candles_after_left_nearest_lower_valley = \
                        prices[prices.index.between(peaks_valleys.index[i] - left_distance , peaks_valleys.index[i])] \
                            .loc[prices['low'] > peaks_valleys.iloc[i]['low']]
                    if len(higher_candles_after_left_nearest_lower_valley) == 0:
                        left_distance = timedelta(0)
            if i < len(peaks_valleys.index.values) - 1:
                right_lower_valleys = prices[prices.index > peaks_valleys.index[i]].loc[prices['low'] <
                                                                                        peaks_valleys.iloc[i]['low']]
                if len(right_lower_valleys.index.values) > 0:
                    right_distance = (right_lower_valleys.index[0] - peaks_valleys.index[i])
                    higher_candles_before_right_nearest_lower_valley = \
                        prices[peaks_valleys.index[i] < prices.index
                               and prices.index < peaks_valleys.index[i] + right_distance] \
                            .loc[prices['low'] > peaks_valleys.iloc[i]['low']]
                    if len(higher_candles_before_right_nearest_lower_valley) == 0:
                        right_distance = timedelta(0)
        else:  # peaks_mode
            if i > 0:
                left_higher_valleys = prices[prices.index < peaks_valleys.index[i]].loc[prices['high'] >
                                                                                        peaks_valleys.iloc[i]['high']]
                if len(left_higher_valleys.index.values) > 0:
                    left_distance = (peaks_valleys.index[i] - left_higher_valleys.index[-1])  #
                    lower_candles_after_left_nearest_higher_peak = \
                        prices[peaks_valleys.index[i] - left_distance < prices.index
                               and prices.index < peaks_valleys.index[i]] \
                            .loc[prices['high'] < peaks_valleys.iloc[i]['high']]
                    if len(lower_candles_after_left_nearest_higher_peak) == 0:
                        left_distance = timedelta(0)
            if i < len(peaks_valleys.index.values) - 1:
                right_higher_valleys = prices[prices.index > peaks_valleys.index[i]].loc[prices['high'] >
                                                                                         peaks_valleys.iloc[i]['high']]
                if len(right_higher_valleys.index.values) > 0:
                    right_distance = (right_higher_valleys.index[0] - peaks_valleys.index[i])  #
                    lower_candles_before_right_nearest_higher_peak = \
                        # todo: left code here!!!
                    # https://stackoverflow.com/questions/22898824/filtering-pandas-dataframes-on-dates
                        prices[peaks_valleys.index[i] < prices.index] \
                               [prices.index < peaks_valleys.index[i] + right_distance] \
                            .loc[prices['high'] < peaks_valleys.iloc[i]['high']]
                    if len(lower_candles_before_right_nearest_higher_peak) == 0:
                        left_distance = timedelta(0)
        if 0 < i < len(peaks_valleys.index.values) - 1 and left_distance == right_distance == INFINITY_TIME_DELTA:
            peaks_valleys.at[peaks_valleys.index[i], 'strength'] \
                = min(peaks_valleys.index[i] - peaks_valleys.index[0],
                      peaks_valleys.iloc[i]['strength'])  # min(i, len(prices) - i)
            continue
        # if DEBUG: print(
        #     f"@{peaks_valleys.index[i]}:min(left_distance:{left_distance}, "
        #     f"right_distance:{right_distance}):{min(left_distance, right_distance)}")
        peaks_valleys.at[peaks_valleys.index[i], 'strength'] = \
            min(left_distance, right_distance, peaks_valleys.iloc[i]['strength'])

    if min_strength is not None:
        raise Exception('Not tested')
        peaks_valleys = peaks_valleys['strength' >= min_strength]
    if ignore_n_percent_lowest_strength is not None:
        raise Exception('Not implemented')
        # todo: extract distribution of strength and ignore n_percent lowest peak_valleys
        # peak_valley_weights = peaks_valleys['strength'].unique().sort(reverse=True)
        # if len(peak_valley_weights) > ignore_n_percent_lowest_strength:
        #     peaks_valleys = peaks_valleys['strength' >= peak_valley_weights[ignore_n_percent_lowest_strength - 1]]
    peaks_valleys = peaks_valleys[peaks_valleys['strength'] > timedelta(0)]
    peaks_valleys.insert(len(peaks_valleys.columns), 'effective_time', None)
    for i in range(len(config.times)):
        for t_peak_valley_index in peaks_valleys[
            peaks_valleys['strength'] > pd.to_timedelta(config.times[i])
        ].index.values:
            peaks_valleys.at[t_peak_valley_index, 'effective_time'] = config.times[i]

    # todo: merge adjacent peaks/valleys and move less significant to lower time.
    peaks_valleys = peaks_valleys[pd.notna(peaks_valleys['effective_time'])]
    return peaks_valleys
