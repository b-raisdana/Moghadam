import pandas as pd
from datetime import timedelta
from typing import List


def find_peaks_valleys(ohlcv_df: pd.DataFrame, timeframes: List[str]):
    peaks_valleys = pd.DataFrame(columns=['peak_or_valley', 'timeframe', 'strength'])

    for timeframe in timeframes:
        resampled_df = ohlcv_df.resample(timeframe).agg({
            'high': 'max',
            'low': 'min'
        })

        peaks_mask = (resampled_df['high'].shift(1) < resampled_df['high']) & (resampled_df['high'].shift(-1) < resampled_df['high'])
        valleys_mask = (resampled_df['low'].shift(1) > resampled_df['low']) & (resampled_df['low'].shift(-1) > resampled_df['low'])

        peaks = resampled_df.loc[peaks_mask, 'high']
        valleys = resampled_df.loc[valleys_mask, 'low']

        if not peaks.empty:
            peaks_df = pd.DataFrame({'peak_or_valley': ['PEAK'] * len(peaks), 'timeframe': [timeframe] * len(peaks),
                                     'strength': [timedelta.max] * len(peaks)}, index=peaks.index)
            peaks_valleys = pd.concat([peaks_valleys, peaks_df])

        if not valleys.empty:
            valleys_df = pd.DataFrame({'peak_or_valley': ['VALLEY'] * len(valleys), 'timeframe': [timeframe] * len(valleys),
                                       'strength': [timedelta.max] * len(valleys)}, index=valleys.index)
            peaks_valleys = pd.concat([peaks_valleys, valleys_df])

    peaks_valleys.sort_index(inplace=True)

    for index, row in peaks_valleys.iterrows():
        is_peak = row['peak_or_valley'] == 'PEAK'
        timeframe = row['timeframe']
        index = row.name

        if is_peak:
            search_df = resampled_df.loc[index:, 'high']
            type_label = 'PEAK'
        else:
            search_df = resampled_df.loc[index:, 'low']
            type_label = 'VALLEY'

        strength = timedelta.max

        for i in range(1, len(search_df)):
            if search_df.iloc[i] != search_df.iloc[i - 1]:
                if (is_peak and search_df.iloc[i] > search_df.iloc[i - 1]) or (not is_peak and search_df.iloc[i] < search_df.iloc[i - 1]):
                    strength = index - search_df.iloc[i - 1].name
                break

        peaks_valleys.at[index, 'strength'] = min(strength, index - ohlcv_df.index[0])

    return peaks_valleys


# Example usage:
# ohlcv_df = ...  # Your OHLCV DataFrame with 'date' as index
# timeframes = config.timeframes
# peaks_n_valleys = find_peaks_valleys(ohlcv_df, timeframes)
