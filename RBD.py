import pandas as pd

PEAK = 'Peak'
VALLEY = 'Valley'


def load_peak_n_valleys() -> pd:
    peaks = pd.read_csv('peaks.17-10-05.18-40T17-10-06.11-19.zip', index_col='date', header=0, parse_dates='date')
    peaks['peak_or_valley'] = PEAK
    valleys = pd.read_csv('valleys.17-10-05.18-40T17-10-06.11-19.zip', index_col='date', header=0, parse_dates='date')
    valleys['peak_or_valley'] = VALLEY
    peaks_n_valleys = pd.concat(peaks, valleys)
    return peaks_n_valleys


BASE_TREND = 'BASE_TREND'
RALLY_TREND = 'RALLY_TREND'
DROP_TREND = 'DROP_TREND'


def rally_base_drop(peaks_n_valleys: pd):
    # base: in reverse trigger time a candle being 80% covered by its previous one.
    # we go out of base if engulf and hunter levels being passed
    if len(peaks_n_valleys['effective_time'].unique()) > 1:
        raise Exception('Expected to peaks_n_valleys filtered and grouped by effective_time before!')
    effective_time = peaks_n_valleys['effective_time'].unique()[0]
    trend = pd.DataFrame(
        index=pd.date_range(start=peaks_n_valleys.index[0], end=peaks_n_valleys.index[-1], freq=effective_time))
    trend['previous_peak'] = \
        peaks_n_valleys.loc[(peaks_n_valleys.peak_or_valley == PEAK) & (peaks_n_valleys.index <= trend.index)][-1]
    trend['next_peak'] = \
        peaks_n_valleys.loc[(peaks_n_valleys.peak_or_valley == PEAK) & (peaks_n_valleys.index >= trend.index)][-0]
    trend['previous_valley'] = \
        peaks_n_valleys.loc[(peaks_n_valleys.peak_or_valley == VALLEY) & (peaks_n_valleys.index <= trend.index)][-1]
    trend['next_valley'] = \
        peaks_n_valleys.loc[(peaks_n_valleys.peak_or_valley == VALLEY) & (peaks_n_valleys.index >= trend.index)][-0]
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
