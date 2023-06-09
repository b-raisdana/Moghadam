import pandas as pd
from plotfig import plotfig
from datetime import timedelta

from LevelDetectionConfig import config

# os.system((f'wget {config.data_path_preamble}/{config.files_to_load[0]}.zip -O {config.files_to_load[0]}.zip'))

reverse_prices = pd.read_csv(f'{config.files_to_load[0]}.zip', index_col='date', parse_dates=['date'])
print(reverse_prices)
# plotfig(reverse_prices.head(10000), name='reverse_prices Head')
# plotfig(reverse_prices.tail(10000), name='reverse_prices Tail')

testprices = reverse_prices.head(100)
print(testprices)
# plotfig(testprices, name='testprices')

DEBUG = True


def level_extractor(prices: pd.DataFrame, min_weight=None, significance=None, max_cycles=100):
    return (
        0,
        # peaks_valleys_extractor(prices, peaks_mode= True , min_weight=min_weight , significance=significance, max_cycles =max_cycles),
        peaks_valleys_extractor(prices, peaks_mode=False, min_weight=min_weight, significance=significance,
                                max_cycles=max_cycles),
    )


def peaks_valleys_extractor(prices: pd.DataFrame, peaks_mode: bool = True, min_weight=None, significance=None,
                            max_cycles=100):
    # Todo: Zero volume candles are not eliminated!
    valleys_mode = not peaks_mode
    peaks_valleys: pd = prices  # pd.DataFrame(prices.index)
    peaks_valleys['weight'] = float('inf')
    if DEBUG: print(
        f"peaks_valleys[peaks_valleys['weight']==float('inf'):{peaks_valleys[peaks_valleys['weight'] == float('inf')]}")
    # infinite_peaks_valleys = peaks_valleys[peaks_valleys['weight'] == float('inf')]
    # while len(infinite_peaks_valleys.index.values) > 1 and max_cycles > 0:
    #     max_cycles -= 1
    if DEBUG: print(
        f"len(peaks_valleys[peaks_valleys['weight']==float('inf')].index.values){len(peaks_valleys[peaks_valleys['weight'] == float('inf')].index.values)}")
    if DEBUG: print(
        f"peaks_valleys[peaks_valleys['weight']==float('inf')]:{peaks_valleys[peaks_valleys['weight'] == float('inf')]}")
    for i in range(1, len(peaks_valleys)):
        if peaks_valleys.iloc[i]['low'] == peaks_valleys.iloc[i - 1]['low']:
            peaks_valleys.at[peaks_valleys.index[i], 'weight'] = 0
    for i in range(len(peaks_valleys.index.values)):
        left_distance = float('inf')
        right_distance = float('inf')

        if valleys_mode:
            if i > 0:
                left_lower_valleys = peaks_valleys[: peaks_valleys.index[i - 1]][peaks_valleys['low'] <
                                                                                 peaks_valleys.iloc[i]['low']]
                if len(left_lower_valleys.index.values) > 0:
                    left_distance = (peaks_valleys.index[i] - left_lower_valleys.index[-1]) / timedelta(minutes=1)
            if i < len(peaks_valleys.index.values) - 1:
                right_lower_valleys = peaks_valleys[peaks_valleys.index[i + 1]:][peaks_valleys['low'] <
                                                                                 peaks_valleys.iloc[i]['low']]
                if len(right_lower_valleys.index.values) > 0:
                    right_distance = (right_lower_valleys.index[0] - peaks_valleys.index[i]) / timedelta(minutes=1)

        if peaks_mode:
            raise Exception('Not implemented!')
        #     if i>0 and peaks_valleys[:i-1][peaks_valleys['low']<peaks_valleys.iloc[i]['low']]
        # if valleys_mode and i > 0 and \
        #         infinite_peaks_valleys.iloc[i]['low'] == infinite_peaks_valleys.iloc[i - 1]['low']:
        #     peaks_valleys.at[infinite_peaks_valleys.index[i], 'weight'] = 0
        #     continue
        # if valleys_mode and i > 0 and \
        #         infinite_peaks_valleys.iloc[i]['low'] > infinite_peaks_valleys.iloc[i - 1]['low']:
        #     left_distance = (infinite_peaks_valleys.index[i] - infinite_peaks_valleys.index[i - 1]) \
        #                     / timedelta(minutes=1)
        # if valleys_mode and i < len(infinite_peaks_valleys.index.values) - 1 and \
        #         infinite_peaks_valleys.iloc[i]['low'] > infinite_peaks_valleys.iloc[i + 1]['low']:
        #     right_distance = (infinite_peaks_valleys.index[i + 1] - infinite_peaks_valleys.index[i]) \
        #                      / timedelta(minutes=1)
        # if peaks_mode and i > 0 and \
        #         infinite_peaks_valleys.iloc[i]['high'] < infinite_peaks_valleys.iloc[i - 1]['high']:
        #     left_distance = (infinite_peaks_valleys.index[i] - infinite_peaks_valleys.index[i - 1]) \
        #                     / timedelta(minutes=1)
        # if peaks_mode and i < len(infinite_peaks_valleys.index.values) - 1 \
        #         and prices.iloc[i]['high'] < prices.iloc[i + 1]['high']:
        #     right_distance = (infinite_peaks_valleys.index[i + 1] - infinite_peaks_valleys.index[i]) \
        #                      / timedelta(minutes=1)
        print(
            f"@{peaks_valleys.index[i]}:min(left_distance:{left_distance}, "
            f"right_distance:{right_distance}):{min(left_distance, right_distance)}")
        if peaks_valleys.iloc[i]['weight']:
            pass
        else:
            pass
        peaks_valleys.at[peaks_valleys.index[i], 'weight'] = \
            min(left_distance, right_distance, peaks_valleys.iloc[i]['weight'])

    if min_weight is not None:
        peaks_valleys = peaks_valleys['weight' >= min_weight]
    if significance is not None:
        peak_valley_weights = peaks_valleys['weight'].unique().sort(reverse=True)
        if len(peak_valley_weights) > significance:
            peaks_valleys = peaks_valleys['weight' >= peak_valley_weights[significance - 1]]
    return peaks_valleys


peaks, valleys = level_extractor(testprices)

print(peaks)
print(valleys)
