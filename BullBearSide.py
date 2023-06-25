import pandas as pd

from Config import TopTYPE, config, TREND
from PeaksValleys import plot_peaks_n_valleys, peaks_only, valleys_only, effective_peak_valleys


def insert_previous_n_next_peaks_n_valleys(peaks_n_valleys, ohlc):
    # Todo: Not tested!
    # any 5min top is major to 1min candles so following condition is not right!
    # if len(peaks_n_valleys['effective_time'].unique()) > 1:
    #     raise Exception('Expected the peaks and valleys be grouped and filtered to see the same value for all rows.')
    ohlc = insert_previous_n_next_top(TopTYPE.PEAK, peaks_n_valleys, ohlc)
    ohlc = insert_previous_n_next_top(TopTYPE.VALLEY, peaks_n_valleys, ohlc)
    return ohlc


def insert_previous_n_next_top(top_type: TopTYPE, peaks_n_valleys, ohlc):
    # Todo: Not tested!
    tops = peaks_n_valleys[peaks_n_valleys['peak_or_valley'] == top_type]
    high_or_low = 'high' if top_type == TopTYPE.PEAK else 'low'
    for i in range(len(tops)):
        if i == len(tops) - 1:
            for j in ohlc.loc[tops.index[i] < ohlc.index].index:
                ohlc.at[j, f'previous_{top_type.value}_index'] = tops.index[i]
                ohlc.at[j, f'previous_{top_type.value}_value'] = tops.iloc[i][high_or_low]
        else:
            for j in ohlc.loc[(tops.index[i] < ohlc.index) & (ohlc.index <= tops.index[i + 1])].index:
                ohlc.at[j, f'previous_{top_type.value}_index'] = tops.index[i]
                ohlc.at[j, f'previous_{top_type.value}_value'] = tops.iloc[i][high_or_low]
            if i == 0:
                for j in ohlc.loc[ohlc.index <= tops.index[i]].index:
                    ohlc.at[j, f'next_{top_type.value}_index'] = tops.index[i]
                    ohlc.at[j, f'next_{top_type.value}_value'] = tops.iloc[i][high_or_low]
            else:
                for j in ohlc.loc[(tops.index[i - 1] < ohlc.index) & (ohlc.index <= tops.index[i])].index:
                    ohlc.at[j, f'next_{top_type.value}_index'] = tops.index[i]
                    ohlc.at[j, f'next_{top_type.value}_value'] = tops.iloc[i][high_or_low]
    return ohlc


def bull_bear_side_for_effective_time(effective_time: str, ohlc: pd.DataFrame, peaks_n_valley: pd.DataFrame):
    # Todo: Not tested!
    if effective_time not in config.times:
        raise Exception(f'Unsupported effective_time:{effective_time} expected to be from: [{config.times}]')
    _effective_peaks_n_valley = effective_peak_valleys(peaks_n_valley, effective_time)
    if any([i not in ohlc.columns for i in
            [f'{j}_{k}' for j in ['previous', 'next'] for k in [e.value for e in TopTYPE]]]):
        ohlc = insert_previous_n_next_peaks_n_valleys(_effective_peaks_n_valley, ohlc)
    ohlc[f'bull_bear_side_{effective_time}'] = TREND.SIDE
    for i in ohlc.loc[(ohlc.next_valley_value > ohlc.previous_valley_value) & (
            ohlc.next_peak_value > ohlc.previous_peak_value)].index:
        ohlc.at[i, f'bull_bear_side_{effective_time}'] = TREND.BULLISH
    for i in ohlc.loc[(ohlc.next_peak_value < ohlc.previous_peak_value) & (
            ohlc.next_valley_value < ohlc.previous_valley_value)].index:
        ohlc.at[i, f'bull_bear_side_{effective_time}'] = TREND.BEARISH
    return ohlc


def bull_bear_side_boundaries(ohlc: pd.DataFrame):
    effective_times = [i.replace('bull_bear_side_', '') for i in ohlc.columns if i.startswith('bull_bear_side_')]
    boundaries = pd.DataFrame()
    for effective_time in effective_times:
        ohlc[f'previous_bull_bear_side_{effective_time}'] = ohlc[f'bull_bear_side_{effective_time}'].shift(1)
        time_boundaries = ohlc[
            ohlc[f'previous_bull_bear_side_{effective_time}'] != ohlc[f'bull_bear_side_{effective_time}']]
        for i in range(len(time_boundaries)):
            # todo: trace to here
            if i < len(time_boundaries) - 1:
                time_boundaries.at[time_boundaries.index[i], 'end'] = ohlc.loc[:time_boundaries.index[i + 1]].index[-1]
            else:
                time_boundaries.at[time_boundaries.index[i], 'end'] = ohlc.index[-1]
        time_boundaries['effective_time'] = effective_time
        pd.concat([boundaries, time_boundaries])
    return boundaries


def boundary_including_peaks_valleys(peaks_n_valleys: pd.DataFrame, boundary_start: pd.Timestamp,
                                     boundary_end: pd.Timestamp):
    return peaks_n_valleys.loc[(peaks_n_valleys.index >= boundary_start) & (peaks_n_valleys.index >= boundary_end)]


def bull_bear_side(ohlc: pd.DataFrame, peaks_n_valley: pd.DataFrame) -> pd.DataFrame:
    for effective_time in peaks_n_valley['effective_time'].unique():
        ohlc = bull_bear_side_for_effective_time(effective_time, ohlc, peaks_n_valley)
    return ohlc


def plot_bull_bear_side(ohlc: pd.DataFrame, peaks_n_valleys: pd.DataFrame, name):
    fig = plot_peaks_n_valleys(ohlc, peaks=peaks_only(peaks_n_valleys), valleys=valleys_only(peaks_n_valleys),
                               name=name)
    _bull_bear_side_boundaries = bull_bear_side_boundaries(ohlc)
    for effective_time in _bull_bear_side_boundaries['effective_time'].unique():
        for boundary_index, boundary in _bull_bear_side_boundaries.iterrows():
            boundary_peaks_n_valleys = boundary_including_peaks_valleys(peaks_n_valleys, boundary.index,
                                                                        boundary['end'])
            boundary_peaks = peaks_only(boundary_peaks_n_valleys)
            boundary_valleys = valleys_only(boundary_peaks_n_valleys)
            xs = [boundary_index, boundary_peaks.index, boundary['end'], boundary_valleys.index]
            ys = [ohlc.loc[boundary_index]['open'], boundary_peaks['high'], ohlc.loc[boundary['end']]['close'],
                  boundary_valleys['low']]
            fill_color = 'green' if boundary[f'bull_bear_side_{effective_time}'] == TREND.BULLISH else 'red' if \
                boundary[f'bull_bear_side_{effective_time}'] == TREND.BEARISH else 'gray'
            fig.add_scatter(x=xs, y=ys, fill="toself", fillcolor=fill_color, name=effective_time)
