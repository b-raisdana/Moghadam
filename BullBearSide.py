import pandas as pd
from pandas import Timestamp

from Config import TopTYPE, config, TREND
from PeaksValleys import plot_peaks_n_valleys, peaks_only, valleys_only, effective_peak_valleys


def insert_previous_n_next_peaks_n_valleys(peaks_n_valleys, ohlc):
    # Todo: Not tested!
    # any 5min top is major to 1min candles so following condition is not right!
    # if len(peaks_n_valleys['timeframe'].unique()) > 1:
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


def candles_trend_single_timeframe(timeframe: str, ohlca: pd.DataFrame, peaks_n_valley: pd.DataFrame,
                                   atr_significance: float = 10 / 100):
    # Todo: Not tested!
    if timeframe not in config.timeframes:
        raise Exception(f'Unsupported timeframe:{timeframe} expected to be from: [{config.timeframes}]')
    _effective_peaks_n_valley = effective_peak_valleys(peaks_n_valley, timeframe)
    if any([i not in ohlca.columns for i in
            [f'{j}_{k}' for j in ['previous', 'next'] for k in [e.value for e in TopTYPE]]]):
        ohlca = insert_previous_n_next_peaks_n_valleys(_effective_peaks_n_valley, ohlca)
    ohlca[f'bull_bear_side_{timeframe}'] = TREND.SIDE
    ohlca.loc[
        (ohlca['next_valley_value'] > ohlca['previous_valley_value']) &
        (ohlca['next_peak_value'] > ohlca['previous_peak_value']) &
        (ohlca['next_peak_index'] > ohlca['next_valley_index'])  # the higher peak should be after higher valley
        , f'bull_bear_side_{timeframe}'] = TREND.BULLISH
    ohlca.loc[
        (ohlca['next_peak_value'] < ohlca['previous_peak_value']) &
        (ohlca['next_valley_value'] < ohlca['previous_valley_value']) &
        (ohlca['next_peak_index'] > ohlca['next_valley_index'])  # the lower valley should be after lower peak
        , f'bull_bear_side_{timeframe}'] = TREND.BEARISH
    return ohlca


def trend_boundaries(ohlc: pd.DataFrame):
    effective_times = [i.replace('bull_bear_side_', '') for i in ohlc.columns if i.startswith('bull_bear_side_')]
    boundaries = pd.DataFrame()
    ohlc['time_of_previous'] = ohlc.index.shift(-1, freq='1min')
    for timeframe in effective_times:
        # todo: move effective time to a column to prevent multiple columns for different effective times.
        ohlc[f'previous_bull_bear_side_{timeframe}'] = ohlc[f'bull_bear_side_{timeframe}'].shift(1)
        time_boundaries = ohlc[
            ohlc[f'previous_bull_bear_side_{timeframe}'] != ohlc[f'bull_bear_side_{timeframe}']]
        time_boundaries['timeframe'] = timeframe
        time_boundaries['bull_bear_side'] = time_boundaries[f'bull_bear_side_{timeframe}']
        unnecessary_columns = [i for i in ohlc.columns if
                               i.startswith('bull_bear_side_') or i.startswith('previous_bull_bear_side_')]
        time_boundaries.drop(columns=unnecessary_columns, inplace=True)
        time_of_last_candle = ohlc.index[-1]
        time_boundaries.loc[:, 'time_of_next'] = time_boundaries.index
        time_boundaries.loc[:, 'time_of_next'] = time_boundaries['time_of_next'].shift(-1)
        time_boundaries.loc[time_boundaries.index[-1], 'time_of_next'] = time_of_last_candle
        time_boundaries.loc[:, 'end'] = ohlc.loc[time_boundaries['time_of_next'], 'time_of_previous'].tolist()
        time_boundaries.drop(columns=['time_of_next'])
        boundaries = pd.concat([boundaries, time_boundaries])
    return boundaries


def boundary_including_peaks_valleys(peaks_n_valleys: pd.DataFrame, boundary_start: pd.Timestamp,
                                     boundary_end: pd.Timestamp):
    return peaks_n_valleys.loc[(peaks_n_valleys.index >= boundary_start) & (peaks_n_valleys.index <= boundary_end)]


def bull_bear_side(ohlca: pd.DataFrame, peaks_n_valley: pd.DataFrame) -> pd.DataFrame:
    for timeframe in peaks_n_valley['timeframe'].unique():
        ohlca = candles_trend_single_timeframe(timeframe, ohlca, peaks_n_valley)
    return ohlca


MAX_NUMBER_OF_PLOT_SCATTERS = 50


def plot_bull_bear_side(ohlc: pd.DataFrame, peaks_n_valleys: pd.DataFrame, boundaries: pd.DataFrame = None,
                        name: str = '', do_not_show: bool = False, html_path: str = ''):
    fig = plot_peaks_n_valleys(ohlc, peaks=peaks_only(peaks_n_valleys), valleys=valleys_only(peaks_n_valleys),
                               name=name, do_not_show=True)
    if boundaries is None:
        boundaries = trend_boundaries(ohlc)
    remained_number_of_scatters = MAX_NUMBER_OF_PLOT_SCATTERS
    for timeframe in boundaries['timeframe'].unique():
        if remained_number_of_scatters <= 0:
            break
        for boundary_index, boundary in boundaries.iterrows():
            if boundary_index == Timestamp('2017-01-04 11:17:00'):
                pass
            boundary_peaks_n_valleys = boundary_including_peaks_valleys(peaks_n_valleys, boundary_index,
                                                                        boundary['end'])
            boundary_peaks = peaks_only(boundary_peaks_n_valleys)
            boundary_valleys = valleys_only(boundary_peaks_n_valleys)
            xs = [boundary_index] + boundary_peaks.index.tolist() + [boundary['end']] + \
                 sorted(boundary_valleys.index.tolist(), reverse=True)
            ys = [ohlc.loc[boundary_index, 'open']] + boundary_peaks['high'].values.tolist() + \
                 [ohlc.loc[boundary['end'], 'close']] + sorted(boundary_valleys['low'].values.tolist(), reverse=True)
            fill_color = 'green' if boundary['bull_bear_side'] == TREND.BULLISH else \
                'red' if boundary['bull_bear_side'] == TREND.BEARISH else 'gray'
            if remained_number_of_scatters > 0:
                fig.add_scatter(x=xs, y=ys, mode="lines", fill="toself",  # fillcolor=fill_color,
                                fillpattern=dict(fgopacity=0.5, shape='.'),
                                name=f'{boundary_index} {timeframe}',
                                line=dict(color=fill_color, width=config.timeframes.index(timeframe) + 1))
                remained_number_of_scatters -= 1
            else:
                break

    if html_path != '':
        figure_as_html = fig.to_html()
        with open(html_path, '+w') as f:
            f.write(figure_as_html)
    if not do_not_show: fig.show()
    return fig
