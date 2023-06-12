from datetime import timedelta

import pandas as pd
from pandas._libs.tslibs import to_offset

from LevelDetectionConfig import config
from plotfig import plotfig

# os.system((f'wget {config.data_path_preamble}/{config.files_to_load[0]}.zip -O {config.files_to_load[0]}.zip'))

reverse_prices = pd.read_csv(f'{config.files_to_load[0]}.zip', index_col='date', parse_dates=['date'])
print(reverse_prices)
# plotfig(reverse_prices.head(10000), name='reverse_prices Head')
# plotfig(reverse_prices.tail(10000), name='reverse_prices Tail')

test_prices = reverse_prices.head(100)
print(test_prices)
plotfig(test_prices, name='test prices', save=False)

DEBUG = False


def level_extractor(prices: pd.DataFrame, min_weight=None, significance=None, max_cycles=100):
    _peaks = peaks_valleys_extractor(prices, True, min_weight, significance, max_cycles)
    _valleys = peaks_valleys_extractor(prices, False, min_weight, significance, max_cycles)
    return _peaks, _valleys


def peaks_valleys_extractor(prices: pd.DataFrame, peaks_mode: bool = True, min_strength: timedelta = None,
                            ignore_n_percent_lowest_strength=None, max_cycles=100):
    # Todo: Zero volume candles are not eliminated!
    valleys_mode = not peaks_mode
    peaks_valleys: pd = prices  # pd.DataFrame(prices.index)
    peaks_valleys.insert(len(peaks_valleys.columns),'strength', timedelta(days=10 * 365))
    if DEBUG: print(
        f"peaks_valleys[peaks_valleys['strength']==float('inf'):{peaks_valleys[peaks_valleys['strength'] == float('inf')]}")
    if DEBUG: print(
        f"len(peaks_valleys[peaks_valleys['strength']==float('inf')].index.values){len(peaks_valleys[peaks_valleys['strength'] == float('inf')].index.values)}")
    if DEBUG: print(
        f"peaks_valleys[peaks_valleys['strength']==float('inf')]:{peaks_valleys[peaks_valleys['strength'] == float('inf')]}")
    for i in range(1, len(peaks_valleys)):
        if valleys_mode:
            if peaks_valleys.iloc[i]['low'] == peaks_valleys.iloc[i - 1]['low']:
                peaks_valleys.at[peaks_valleys.index[i], 'strength'] = timedelta(0)
        else:  # peaks_mode
            if peaks_valleys.iloc[i]['high'] == peaks_valleys.iloc[i - 1]['high']:
                peaks_valleys.at[peaks_valleys.index[i], 'strength'] = timedelta(0)
    for i in range(len(peaks_valleys.index.values)):
        left_distance = timedelta(days=10 * 365)
        right_distance = timedelta(days=10 * 365)

        if valleys_mode:
            if i > 0:
                left_lower_valleys = peaks_valleys[: peaks_valleys.index[i - 1]].loc[peaks_valleys['low'] <
                                                                                     peaks_valleys.iloc[i]['low']]
                if len(left_lower_valleys.index.values) > 0:
                    left_distance = (peaks_valleys.index[i] - left_lower_valleys.index[-1])  #
                    # / timedelta(minutes=1) - 1
            if i < len(peaks_valleys.index.values) - 1:
                right_lower_valleys = peaks_valleys[peaks_valleys.index[i + 1]:].loc[peaks_valleys['low'] <
                                                                                     peaks_valleys.iloc[i]['low']]
                if len(right_lower_valleys.index.values) > 0:
                    right_distance = (right_lower_valleys.index[0] - peaks_valleys.index[i])  #
                    # / timedelta(minutes=1) - 1

        else:  # peaks_mode
            if i > 0:
                left_lower_valleys = peaks_valleys[: peaks_valleys.index[i - 1]].loc[peaks_valleys['high'] >
                                                                                     peaks_valleys.iloc[i]['high']]
                if len(left_lower_valleys.index.values) > 0:
                    left_distance = (peaks_valleys.index[i] - left_lower_valleys.index[-1])  #
                    # / timedelta(minutes=1) - 1
            if i < len(peaks_valleys.index.values) - 1:
                right_lower_valleys = peaks_valleys[peaks_valleys.index[i + 1]:].loc[peaks_valleys['high'] >
                                                                                     peaks_valleys.iloc[i]['high']]
                if len(right_lower_valleys.index.values) > 0:
                    right_distance = (right_lower_valleys.index[0] - peaks_valleys.index[i])  #
                    # / timedelta(minutes=1) - 1
            if 0 < i < len(peaks_valleys.index.values) - 1 and left_distance == right_distance == timedelta(
                    days=10 * 365):
                peaks_valleys.at[peaks_valleys.index[i], 'strength'] = peaks_valleys.index[i] - peaks_valleys.index[
                    0]  # min(i, len(prices) - i)
                continue
        if DEBUG: print(
            f"@{peaks_valleys.index[i]}:min(left_distance:{left_distance}, "
            f"right_distance:{right_distance}):{min(left_distance, right_distance)}")
        if peaks_valleys.iloc[i]['strength']:
            pass
        else:
            pass
        peaks_valleys.at[peaks_valleys.index[i], 'strength'] = \
            min(left_distance, right_distance, peaks_valleys.iloc[i]['strength'])

    if min_strength is not None:
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
            peaks_valleys['strength'] >= pd.to_timedelta(config.times[i]) * 2
        ].index.values:
            peaks_valleys.at[t_peak_valley_index, 'effective_time'] = pd.to_timedelta(config.times[i])
    peaks_valleys = peaks_valleys[peaks_valleys['effective_time'] is not None]
    return peaks_valleys


peaks, valleys = level_extractor(test_prices)

print('Peaks:\n', peaks.sort_values(by='strength', ascending=False))
print('Valleys\n', valleys.sort_values(by='strength', ascending=False))
