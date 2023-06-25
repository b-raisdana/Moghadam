import pandas as pd

from Config import PEAK, VALLEY
from PeaksValleys import merge_tops


# todo: left uncompleted


def load_peak_n_valleys() -> pd:
    peaks = pd.read_csv('peaks.17-10-05.18-40T17-10-06.11-19.zip', index_col='date', header=0, parse_dates='date')
    peaks['peak_or_valley'] = PEAK
    valleys = pd.read_csv('valleys.17-10-05.18-40T17-10-06.11-19.zip', index_col='date', header=0, parse_dates='date')
    valleys['peak_or_valley'] = VALLEY
    peaks_n_valleys = merge_tops(peaks, valleys)
    return peaks_n_valleys


def rally_base_drop(peaks_n_valleys: pd):
    if len(peaks_n_valleys['effective_time'].unique()) > 1:
        raise Exception('Expected to peaks_n_valleys filtered and grouped by effective_time before!')
    effective_time = peaks_n_valleys['effective_time'].unique()[0]
    trend = pd.DataFrame(
        index=pd.date_range(start=peaks_n_valleys.index[0], end=peaks_n_valleys.index[-1], freq=effective_time))
    add_previous_n_next_peaks_n_valleys(peaks_n_valleys, trend)
    trend['trend'] = RALLY_TREND if (
            trend.next_valley > trend.previous_valley and trend.previous_peak > trend.next_peak) \
        else DROP_TREND if (trend.next_valley < trend.previous_valley and trend.previous_peak < trend.next_peak) \
        else BASE_TREND

    # for i, peak_or_valley in peaks_n_valleys.iterrows():
    #     if trend == RALLY_TREND:
    #         pass
    #     elif trend == BASE_TREND:
    #         pass
    #     elif trend == DROP_TREND:
    #         pass
    #     else:
    #         raise Exception(f'Unsupported trend:{trend}')


