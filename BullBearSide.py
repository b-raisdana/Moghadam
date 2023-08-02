import os
from typing import List

import pandas as pd
from pandas import Timestamp
from plotly import graph_objects as plgo

from Config import TopTYPE, config, TREND
from DataPreparation import read_file, read_multi_timeframe_ohlc, single_timeframe
from FigurePlotters import plot_multiple_figures
from PeaksValleys import plot_peaks_n_valleys, peaks_only, valleys_only, read_multi_timeframe_peaks_n_valleys, \
    major_peaks_n_valleys
from helper import log


def insert_previous_n_next_tops(single_timeframe_peaks_n_valleys, ohlc):
    # Todo: Not tested!
    # any 5min top is major to 1min candles so following condition is not right!
    # if len(peaks_n_valleys['timeframe'].unique()) > 1:
    #     raise Exception('Expected the peaks and valleys be grouped and filtered to see the same value for all rows.')
    ohlc = insert_previous_n_next_top(TopTYPE.PEAK, single_timeframe_peaks_n_valleys, ohlc)
    ohlc = insert_previous_n_next_top(TopTYPE.VALLEY, single_timeframe_peaks_n_valleys, ohlc)
    return ohlc


# def insert_previous_n_next_top(top_type: TopTYPE, peaks_n_valleys, ohlc):
#     tops = peaks_n_valleys[peaks_n_valleys['peak_or_valley'] == top_type.value]
#     high_or_low = 'high' if top_type == TopTYPE.PEAK else 'low'
#     for i in range(len(tops)):
# if i == len(tops) - 1:
#     for j in ohlc.loc[tops.index[i] < ohlc.index].index:
#         ohlc.at[j, f'previous_{top_type.value}_index'] = tops.index[i]
#         ohlc.at[j, f'previous_{top_type.value}_value'] = tops.iloc[i][high_or_low]
# else:
#     for j in ohlc.loc[(tops.index[i] < ohlc.index) & (ohlc.index <= tops.index[i + 1])].index:
#         ohlc.at[j, f'previous_{top_type.value}_index'] = tops.index[i]
#         ohlc.at[j, f'previous_{top_type.value}_value'] = tops.iloc[i][high_or_low]
#     if i == 0:
#         for j in ohlc.loc[ohlc.index <= tops.index[i]].index:
#             ohlc.at[j, f'next_{top_type.value}_index'] = tops.index[i]
#             ohlc.at[j, f'next_{top_type.value}_value'] = tops.iloc[i][high_or_low]
#     else:
#         for j in ohlc.loc[(tops.index[i - 1] < ohlc.index) & (ohlc.index <= tops.index[i])].index:
#             ohlc.at[j, f'next_{top_type.value}_index'] = tops.index[i]
#             ohlc.at[j, f'next_{top_type.value}_value'] = tops.iloc[i][high_or_low]
def insert_previous_n_next_top(top_type: TopTYPE, peaks_n_valleys, ohlc):
    # Todo: Not tested!
    if f'previous_{top_type.value}_index' not in ohlc.columns:
        ohlc[f'previous_{top_type.value}_index'] = None
    if f'previous_{top_type.value}_value' not in ohlc.columns:
        ohlc[f'previous_{top_type.value}_value'] = None
    if f'next_{top_type.value}_index' not in ohlc.columns:
        ohlc[f'next_{top_type.value}_index'] = None
    if f'next_{top_type.value}_value' not in ohlc.columns:
        ohlc[f'next_{top_type.value}_value'] = None
    tops = peaks_n_valleys[peaks_n_valleys['peak_or_valley'] == top_type.value]
    high_or_low = 'high' if top_type == TopTYPE.PEAK else 'low'
    for i in range(len(tops)):
        if i == len(tops) - 1:
            is_previous_for_indexes = ohlc.loc[ohlc.index > tops.index.get_level_values('date')[i]].index
        else:
            is_previous_for_indexes = ohlc.loc[(ohlc.index > tops.index.get_level_values('date')[i]) &
                                               (ohlc.index <= tops.index.get_level_values('date')[i + 1])].index
        if i == 0:
            is_next_for_indexes = ohlc.loc[ohlc.index <= tops.index.get_level_values('date')[i]].index
        else:
            is_next_for_indexes = ohlc.loc[(ohlc.index > tops.index.get_level_values('date')[i - 1]) &
                                           (ohlc.index <= tops.index.get_level_values('date')[i])].index
        ohlc.loc[is_previous_for_indexes, f'previous_{top_type.value}_index'] = tops.index.get_level_values('date')[i]
        ohlc.loc[is_previous_for_indexes, f'previous_{top_type.value}_value'] = tops.iloc[i][high_or_low]
        ohlc.loc[is_next_for_indexes, f'next_{top_type.value}_index'] = tops.index.get_level_values('date')[i]
        ohlc.loc[is_next_for_indexes, f'next_{top_type.value}_value'] = tops.iloc[i][high_or_low]
    return ohlc


# def single_timeframe_candles_trend(ohlc: pd.DataFrame, single_timeframe_peaks_n_valley: pd.DataFrame) -> pd.DataFrame:
#     # Todo: Not tested!
#     # if timeframe not in config.timeframes:
#     #     raise Exception(f'Unsupported timeframe:{timeframe} expected to be from: [{config.timeframes}]')
#     # _higher_or_eq_timeframe_peaks_n_valleys = higher_or_eq_timeframe_peaks_n_valleys(peaks_n_valley, timeframe)
#     # if any([i not in ohlc.columns for i in
#     #         [f'{j}_{k}' for j in ['previous', 'next'] for k in [e.value for e in TopTYPE]]]):
#     #     ohlc = insert_previous_n_next_peaks_n_valleys(_higher_or_eq_timeframe_peaks_n_valleys, ohlc)
#     # ohlc[f'bull_bear_side_{timeframe}'] = TREND.SIDE
#     # ohlc.loc[
#     #     (ohlc['next_valley_value'] > ohlc['previous_valley_value']) &
#     #     (ohlc['next_peak_value'] > ohlc['previous_peak_value']) &
#     #     (ohlc['next_peak_index'] > ohlc['next_valley_index'])  # the higher peak should be after higher valley
#     #     , f'bull_bear_side_{timeframe}'] = TREND.BULLISH
#     # ohlc.loc[
#     #     (ohlc['next_peak_value'] < ohlc['previous_peak_value']) &
#     #     (ohlc['next_valley_value'] < ohlc['previous_valley_value']) &
#     #     (ohlc['next_peak_index'] > ohlc['next_valley_index'])  # the lower valley should be after lower peak
#     #     , f'bull_bear_side_{timeframe}'] = TREND.BEARISH
#     # return ohlc
#     # if timeframe not in config.timeframes:
#     #     raise Exception(f'Unsupported timeframe:{timeframe} expected to be from: [{config.timeframes}]')
#     candle_trend = insert_previous_n_next_tops(single_timeframe_peaks_n_valley, ohlc)
#     candle_trend['bull_bear_side'] = TREND.SIDE.value
#     # candle_trend = pd.DataFrame([TREND.SIDE.value] * len(ohlc_with_previous_n_next_tops),
#     #                             index=ohlc_with_previous_n_next_tops.index,
#     #                             columns=['bull_bear_side'])
#     candle_trend.loc[candle_trend.index[
#         (candle_trend['next_valley_value'] > candle_trend[
#             'previous_valley_value']) &
#         (candle_trend['next_peak_value'] > candle_trend['previous_peak_value']) &
#         (candle_trend['next_peak_index'] > candle_trend[
#             'next_valley_index'])]  # the higher peak should be after higher valley
#     , 'bull_bear_side'] = TREND.BULLISH.value
#     candle_trend.loc[candle_trend.index[
#         (candle_trend['next_peak_value'] < candle_trend['previous_peak_value']) &
#         (candle_trend['next_valley_value'] < candle_trend[
#             'previous_valley_value']) &
#         (candle_trend['next_peak_index'] > candle_trend[
#             'next_valley_index'])]  # the lower valley should be after lower peak
#     , 'bull_bear_side'] = TREND.BEARISH.value
#     return candle_trend
def single_timeframe_candles_trend(ohlc: pd.DataFrame, single_timeframe_peaks_n_valley: pd.DataFrame) -> pd.DataFrame:
    # Todo: Not tested!
    candle_trend = insert_previous_n_next_tops(single_timeframe_peaks_n_valley, ohlc)
    candle_trend['bull_bear_side'] = TREND.SIDE.value
    candle_trend.loc[
        candle_trend.index[
            (candle_trend['next_valley_value'] > candle_trend['previous_valley_value'])
            & (candle_trend['next_peak_value'] > candle_trend['previous_peak_value'])
            # & (candle_trend['next_peak_index'] > candle_trend['next_valley_index'])
            ],  # the higher peak should be after higher valley
        'bull_bear_side'] = TREND.BULLISH.value
    candle_trend.loc[
        candle_trend.index[
            (candle_trend['next_peak_value'] < candle_trend['previous_peak_value'])
            & (candle_trend['next_valley_value'] < candle_trend['previous_valley_value'])
            # & (candle_trend['next_peak_index'] > candle_trend['next_valley_index'])
            ],  # the lower valley should be after lower peak
        'bull_bear_side'] = TREND.BEARISH.value
    return candle_trend


# def multi_timeframe_trend_boundaries(multi_timeframe_candle_trend: pd.DataFrame,
#                                      multi_timeframe_peaks_n_valleys: pd.DataFrame,
#                                      timeframe_shortlist: List['str'] = None):
# self.multi_timeframe_trend_boundaries_columns = ['timeframe', 'end', 'bull_bear_side',
#                                                  'highest_hig', 'lowest_low', 'high_time', 'low_time',
#                                                  'trend_line_acceleration', 'trend_line_base',
#                                                  'canal_line_acceleration', 'canal_line_base',
#                                                  ]
# timeframes = [i.replace('bull_bear_side_', '') for i in multi_timeframe_candle_trend.columns if
#                    i.startswith('bull_bear_side_')]
# for _, timeframe in enumerate(config.structure_timeframes):
#     if f'previous_bull_bear_side_{timeframe}' not in multi_timeframe_candle_trend.columns:
#         raise Exception(
#             f'previous_bull_bear_side_{timeframe} not found in candle_trend:({multi_timeframe_candle_trend.columns})')
# boundaries = pd.DataFrame()
# multi_timeframe_candle_trend['time_of_previous'] = multi_timeframe_candle_trend.index.shift(-1, freq='1min')
# for timeframe in timeframes:
#     zztodo: move effective time to a column to prevent multiple columns for different effective times.
#     multi_timeframe_candle_trend.loc[timeframe, 'previous_bull_bear_side'] = multi_timeframe_candle_trend[
#         f'bull_bear_side_{timeframe}'].shift(1)
#     time_boundaries = multi_timeframe_candle_trend[
#         multi_timeframe_candle_trend[f'previous_bull_bear_side_{timeframe}'] != multi_timeframe_candle_trend[
#             f'bull_bear_side_{timeframe}']]
#     time_boundaries['timeframe'] = timeframe
#     time_boundaries['bull_bear_side'] = time_boundaries[f'bull_bear_side_{timeframe}']
#     unnecessary_columns = [i for i in multi_timeframe_candle_trend.columns if
#                            i.startswith('bull_bear_side_') or i.startswith('previous_bull_bear_side_')]
#     time_boundaries.drop(columns=unnecessary_columns, inplace=True)
#     time_of_last_candle = multi_timeframe_candle_trend.index[-1]
#     time_boundaries.loc[:, 'time_of_next'] = time_boundaries.index
#     time_boundaries.loc[:, 'time_of_next'] = time_boundaries['time_of_next'].shift(-1)
#     time_boundaries.loc[time_boundaries.index[-1], 'time_of_next'] = time_of_last_candle
#     time_boundaries.loc[:, 'end'] = multi_timeframe_candle_trend.loc[
#         time_boundaries['time_of_next'], 'time_of_previous'].tolist()
#     time_boundaries.drop(columns=['time_of_next'])
#     boundaries = pd.concat([boundaries, time_boundaries])
def multi_timeframe_trend_boundaries(multi_timeframe_candle_trend: pd.DataFrame,
                                     multi_timeframe_peaks_n_valleys: pd.DataFrame,
                                     timeframe_shortlist: List['str'] = None):
    boundaries = pd.DataFrame()
    if timeframe_shortlist is None:
        timeframe_shortlist = config.timeframes
    for timeframe in timeframe_shortlist:
        single_timeframe_candle_trend = single_timeframe(multi_timeframe_candle_trend, timeframe)
        if len(single_timeframe_candle_trend) == 0:
            log(f'multi_timeframe_candle_trend has no rows for timeframe:{timeframe}')
            continue
        single_timeframe_peaks_n_valleys = major_peaks_n_valleys(multi_timeframe_peaks_n_valleys, timeframe)
        _timeframe_trend_boundaries = single_timeframe_trend_boundaries(single_timeframe_candle_trend,
                                                                        single_timeframe_peaks_n_valleys)
        _timeframe_trend_boundaries['timeframe'] = timeframe
        _timeframe_trend_boundaries.set_index('timeframe', append=True, inplace=True)
        _timeframe_trend_boundaries = _timeframe_trend_boundaries.swaplevel()
        boundaries = pd.concat([boundaries, _timeframe_trend_boundaries])
    boundaries = boundaries[
        [column for column in config.multi_timeframe_trend_boundaries_columns if column != 'timeframe']]
    return boundaries


def add_highest_high_n_lowest_low(_boundaries, single_timeframe_candle_trend):
    if 'highest_high' not in _boundaries.columns:
        # add highest_high to _boundaries.columns
        _boundaries['highest_high'] = None
    if 'lowest_low' not in _boundaries.columns:
        _boundaries['lowest_low'] = None
    for i, _boundary in _boundaries.iterrows():
        _boundaries.loc[i, 'highest_high'] = single_timeframe_candle_trend.loc[i:_boundary['end'], 'high'].max()
        _boundaries.loc[i, 'high_time'] = single_timeframe_candle_trend.loc[i:_boundary['end'], 'high'].idxmax()
        _boundaries.loc[i, 'lowest_low'] = single_timeframe_candle_trend.loc[i:_boundary['end'], 'low'].min()
        _boundaries.loc[i, 'low_time'] = single_timeframe_candle_trend.loc[i:_boundary['end'], 'low'].idxmax()
    return _boundaries


def most_two_significant_tops(start, end, single_timeframe_peaks_n_valleys, tops_type: TopTYPE) -> pd.DataFrame:
    # todo: test most_two_significant_valleys
    filtered_valleys = single_timeframe_peaks_n_valleys.loc[
        (single_timeframe_peaks_n_valleys.index >= start) &
        (single_timeframe_peaks_n_valleys.index <= end) &
        (single_timeframe_peaks_n_valleys['peak_or_valley'] == tops_type.value)
        ].sort_values(by='strength')
    return filtered_valleys.iloc[:2].sort_index()


def add_canal_lines(_boundaries, single_timeframe_peaks_n_valleys):
    # todo: test add_canal_lines
    if _boundaries is None:
        return _boundaries
    for i, _boundary in _boundaries.iterrows():
        if _boundary['bull_bear_side'] == TREND.SIDE.value:
            continue
        _peaks = most_two_significant_tops(i, _boundary['end'], single_timeframe_peaks_n_valleys, TopTYPE.PEAK)
        _valleys = most_two_significant_tops(i, _boundary['end'], single_timeframe_peaks_n_valleys, TopTYPE.VALLEY)
        if _boundary['bull_bear_side'] == TREND.BULLISH.value:
            canal_top = _peaks.iloc[0]['high']
            trend_tops = _valleys['low'].to_numpy()
        else:
            canal_top = _valleys.iloc[0]['low']
            trend_tops = _peaks['high'].to_numpy()
        trend_acceleration = (trend_tops[1][1] - trend_tops[0][1]) / (trend_tops[1][0] - trend_tops[0][0])
        trend_base = trend_tops[0][1] - trend_tops[0][0] * trend_acceleration
        canal_acceleration = trend_acceleration
        canal_base = canal_top[0][1] - canal_top[0][0] * canal_acceleration
        _boundaries.iloc[i, ['trend_acceleration', 'trend_base', 'canal_acceleration', 'canal_base']] = \
            [trend_acceleration, trend_base, canal_acceleration, canal_base]
    return _boundaries


def single_timeframe_trend_boundaries(single_timeframe_candle_trend: pd.DataFrame,
                                      single_timeframe_peaks_n_valleys) -> pd.DataFrame:
    _boundaries = detect_boundaries(single_timeframe_candle_trend)
    _boundaries = add_highest_high_n_lowest_low(_boundaries, single_timeframe_candle_trend)
    # _boundaries = add_canal_lines(_boundaries, single_timeframe_peaks_n_valleys)
    _boundaries = _boundaries[[i for i in config.multi_timeframe_trend_boundaries_columns if i != 'timeframe']]
    return _boundaries


def detect_boundaries(single_timeframe_candle_trend):
    if 'time_of_previous' not in single_timeframe_candle_trend.columns:
        single_timeframe_candle_trend['time_of_previous'] = None
    single_timeframe_candle_trend.loc[1:, 'time_of_previous'] = single_timeframe_candle_trend.index[:-1]
    # single_timeframe_candle_trend['time_of_previous'] = single_timeframe_candle_trend.index.shift(-1, freq=timeframe)
    single_timeframe_candle_trend['previous_trend'] = single_timeframe_candle_trend['bull_bear_side'].shift(1)
    _boundaries = single_timeframe_candle_trend[
        single_timeframe_candle_trend['previous_trend'] != single_timeframe_candle_trend['bull_bear_side']]
    time_of_last_candle = single_timeframe_candle_trend.index[-1]
    # _boundaries['time_of_next'] = _boundaries.index.shift(-1, freq=timeframe)
    if 'end' not in single_timeframe_candle_trend.columns:
        single_timeframe_candle_trend['end'] = None
    _boundaries.loc[:-1, 'end'] = _boundaries.index[1:]
    # _boundaries.loc[:, 'time_of_next'] = _boundaries['time_of_next'].shift(-1)
    _boundaries.loc[_boundaries.index[-1], 'end'] = time_of_last_candle
    # _boundaries['end'] = single_timeframe_candle_trend.loc[_boundaries['time_of_next'], 'time_of_previous'] \
    #     .tolist()
    return _boundaries[['bull_bear_side', 'end']]


def boundary_including_peaks_valleys(peaks_n_valleys: pd.DataFrame, boundary_start: pd.Timestamp,
                                     boundary_end: pd.Timestamp):
    return peaks_n_valleys.loc[(peaks_n_valleys.index.get_level_values('date') >= boundary_start) &
                               (peaks_n_valleys.index.get_level_values('date') <= boundary_end)]


MAX_NUMBER_OF_PLOT_SCATTERS = 50


def plot_single_timeframe_trend_boundaries(ohlc: pd.DataFrame, peaks_n_valleys: pd.DataFrame, boundaries: pd.DataFrame,
                                           name: str = '', show: bool = True,
                                           html_path: str = '', save: bool = True) -> plgo.Figure:
    fig = plot_peaks_n_valleys(ohlc, peaks=peaks_only(peaks_n_valleys), valleys=valleys_only(peaks_n_valleys),
                               name=name, show=False, save=False)
    remained_number_of_scatters = MAX_NUMBER_OF_PLOT_SCATTERS
    # for timeframe in config.timeframes:

    # if remained_number_of_scatters <= 0:
    #     log(f'MAX_NUMBER_OF_PLOT_SCATTERS({MAX_NUMBER_OF_PLOT_SCATTERS}) reached '
    #         f'and we wont add more scatters to plot!')
    #     break
    for boundary_index, boundary in boundaries.iterrows():
        if boundary_index == Timestamp('2017-01-04 11:17:00'):
            pass
        boundary_peaks_n_valleys = boundary_including_peaks_valleys(peaks_n_valleys, boundary_index,
                                                                    boundary['end'])
        boundary_peaks = peaks_only(boundary_peaks_n_valleys)['high'].sort_index(level='date')
        boundary_valleys = valleys_only(boundary_peaks_n_valleys)['low'].sort_index(level='date', ascending=False)
        # xs = [boundary_index] + boundary_peaks.index.get_level_values('date').tolist() + [boundary['end']] + \
        #      sorted(boundary_valleys.index.get_level_values('date').tolist(), reverse=True)
        # ys = [ohlc.loc[boundary_index, 'open']] + boundary_peaks['high'].values.tolist() + \
        #      [ohlc.loc[boundary['end'], 'close']] + sorted(boundary_valleys['low'].values.tolist(), reverse=True)
        xs = [boundary_index] + boundary_peaks.index.get_level_values('date').tolist() + \
             [boundary['end']] + boundary_valleys.index.get_level_values('date').tolist()
        ys = [ohlc.loc[boundary_index, 'open']] + boundary_peaks.values.tolist() + \
             [ohlc.loc[boundary['end'], 'close']] + boundary_valleys.values.tolist()
        fill_color = 'green' if boundary['bull_bear_side'] == TREND.BULLISH.value else \
            'red' if boundary['bull_bear_side'] == TREND.BEARISH.value else 'gray'
        if remained_number_of_scatters > 0:
            fig.add_scatter(x=xs, y=ys, mode="lines", fill="toself",  # fillcolor=fill_color,
                            fillpattern=dict(fgopacity=0.5, shape='.'),
                            name=f"{boundary_index}",  # :(xs{[x.strftime('%m-%d|%H:%M') for x in xs]}, ys{ys})",
                            line=dict(color=fill_color))
            remained_number_of_scatters -= 1
        else:
            break

    if save or html_path != '':
        if html_path == '':
            html_path = os.path.join(config.path_of_plots,
                                     f'single_timeframe_trend_boundaries.{ohlc.index[0].strftime("%y-%m-%d.%H-%M")}T' \
                                     f'{ohlc.index[-1].strftime("%y-%m-%d.%H-%M")}.html')
        figure_as_html = fig.to_html()
        with open(html_path, '+w') as f:
            f.write(figure_as_html)
    if show: fig.show()
    return fig


def read_multi_timeframe_trend_boundaries(date_range_str: str):
    return read_file(date_range_str, 'multi_timeframe_trend_boundaries', generate_multi_timeframe_trend_boundaries)


def plot_multi_timeframe_trend_boundaries(multi_timeframe_ohlc, multi_timeframe_peaks_n_valleys,
                                          _multi_timeframe_trend_boundaries, show: bool = True,
                                          timeframe_shortlist: List['str'] = None):
    figures = []
    if timeframe_shortlist is None:
        timeframe_shortlist = config.timeframes
    for timeframe in timeframe_shortlist:
        _figure = plot_single_timeframe_trend_boundaries(
            ohlc=single_timeframe(multi_timeframe_ohlc, timeframe),
            peaks_n_valleys=major_peaks_n_valleys(multi_timeframe_peaks_n_valleys, timeframe).sort_index(level='date'),
            boundaries=single_timeframe(_multi_timeframe_trend_boundaries, timeframe).sort_index(level='date'),
            show=False, save=False,
            name=f'{timeframe} boundaries')
        figures.append(_figure)
    plot_multiple_figures(figures, file_name=f'multi_timeframe_trend_boundaries.'
                                             f'{multi_timeframe_ohlc.index[0][1].strftime("%y-%m-%d.%H-%M")}T'
                                             f'{multi_timeframe_ohlc.index[-1][1].strftime("%y-%m-%d.%H-%M")}.html',
                          show=show)


def generate_multi_timeframe_trend_boundaries(date_range_str: str, file_path: str = config.path_of_data,
                                              timeframe_short_list: List['str'] = None):
    multi_timeframe_ohlc = read_multi_timeframe_ohlc(date_range_str)
    multi_timeframe_peaks_n_valleys = read_multi_timeframe_peaks_n_valleys(date_range_str)
    # Generate multi-timeframe candle trend
    multi_timeframe_candle_trend = generate_multi_timeframe_candle_trend(date_range_str,
                                                                         timeframe_shortlist=timeframe_short_list)
    # Generate multi-timeframe trend boundaries
    trend_boundaries = multi_timeframe_trend_boundaries(multi_timeframe_candle_trend, multi_timeframe_peaks_n_valleys,
                                                        timeframe_shortlist=timeframe_short_list)
    # Plot multi-timeframe trend boundaries
    plot_multi_timeframe_trend_boundaries(multi_timeframe_ohlc, multi_timeframe_peaks_n_valleys, trend_boundaries,
                                          timeframe_shortlist=timeframe_short_list)
    # Save multi-timeframe trend boundaries to a.zip file
    trend_boundaries.to_csv(os.path.join(file_path, f'multi_timeframe_trend_boundaries.{date_range_str}.zip'),
                            compression='zip')


# def read_multi_timeframe_candle_trend(date_range_str: str):
#     return read_file(date_range_str, 'multi_timeframe_candle_trend', generate_multi_timeframe_candle_trend)

def plot_single_timeframe_candle_trend(ohlc: pd.DataFrame, single_timeframe_candle_trend: pd.DataFrame,
                                       single_timeframe_peaks_n_valleys: pd.DataFrame, show=True, save=True,
                                       path_of_plot=config.path_of_plots, name='Single Timeframe Candle Trend'):
    """
    Plot candlesticks with highlighted trends (Bullish, Bearish, Side).

    The function uses the provided DataFrame containing candle trends and highlights the bars of candles based on
    their trend. Bullish candles are displayed with 70% transparent green color, Bearish candles with 70% transparent red,
    and Side candles with 70% transparent grey color.

    Parameters:
        ohlc (pd.DataFrame): DataFrame containing OHLC data.
        single_timeframe_candle_trend (pd.DataFrame): DataFrame containing candle trend data.
        single_timeframe_peaks_n_valleys (pd.DataFrame): DataFrame containing peaks and valleys data.
        show (bool): If True, the plot will be displayed.
        save (bool): If True, the plot will be saved as an HTML file.
        path_of_plot (str): Path to save the plot.
        name (str): The title of the figure.

    Returns:
        plgo.Figure: The Plotly figure object containing the plot with highlighted trends.
    """
    # Calculate the trend colors
    trend_colors = single_timeframe_candle_trend['bull_bear_side'].map({
        TREND.BULLISH.value: 'rgba(0, 128, 0, 0.7)',  # 70% transparent green for Bullish trend
        TREND.BEARISH.value: 'rgba(255, 0, 0, 0.7)',  # 70% transparent red for Bearish trend
        TREND.SIDE.value: 'rgba(128, 128, 128, 0.7)'  # 70% transparent grey for Side trend
    })

    # Create the figure using plot_peaks_n_valleys function
    fig = plot_peaks_n_valleys(ohlc,
                               peaks=peaks_only(single_timeframe_peaks_n_valleys),
                               valleys=valleys_only(single_timeframe_peaks_n_valleys),
                               name=f'{name} Peaks n Valleys')

    # Update the bar trace with trend colors
    fig.update_traces(marker=dict(color=trend_colors), selector=dict(type='bar'))

    # Set the title of the figure
    fig.update_layout(title_text=name)

    # Show the figure or write it to an HTML file
    if save:
        if not os.path.exists(path_of_plot):
            os.mkdir(path_of_plot)
        file_name = f'{name}.html'
        file_path = os.path.join(path_of_plot, file_name)
        fig.write_html(file_path)

    if show:
        fig.show()

    return fig


def plot_multi_timeframe_candle_trend(multi_timeframe_candle_trend, multi_timeframe_peaks_n_valleys, ohlc, show=True,
                                      save=True, path_of_plot=config.path_of_plots):
    # todo: test plot_multi_timeframe_candle_trend
    figures = []
    _multi_timeframe_peaks = peaks_only(multi_timeframe_peaks_n_valleys)
    _multi_timeframe_valleys = valleys_only(multi_timeframe_peaks_n_valleys)
    for _, timeframe in enumerate(config.timeframes):
        figures.append(
            plot_single_timeframe_candle_trend(ohlc, single_timeframe(multi_timeframe_candle_trend, timeframe),
                                               major_peaks_n_valleys(multi_timeframe_peaks_n_valleys, timeframe),
                                               show=True,
                                               save=True,
                                               path_of_plot=path_of_plot, name=f'{timeframe} Candle Trend'))
    plot_multiple_figures(figures, file_name='multi_timeframe_candle_trend', show=show, save=save,
                          path_of_plot=path_of_plot)


def generate_multi_timeframe_candle_trend(date_range_str: str, timeframe_short_list: List[str] = None,
                                          timeframe_shortlist: List['str'] = None):
    # def candles_trend_multi_timeframe(ohlca: pd.DataFrame, peaks_n_valleys: pd.DataFrame) -> pd.DataFrame:
    #     for timeframe in peaks_n_valleys['timeframe'].unique():
    #         ohlca = candles_trend_single_timeframe(timeframe, ohlca, peaks_n_valleys)
    #     return ohlca
    multi_timeframe_ohlc = read_multi_timeframe_ohlc(date_range_str)
    multi_timeframe_peaks_n_valleys = read_multi_timeframe_peaks_n_valleys(date_range_str).sort_index(level='date')
    multi_timeframe_candle_trend = pd.DataFrame()
    if timeframe_shortlist is None:
        timeframe_shortlist = config.timeframes
    for timeframe in timeframe_shortlist:  # peaks_n_valleys.index.unique(level='timeframe'):
        _timeframe_candle_trend = \
            single_timeframe_candles_trend(single_timeframe(multi_timeframe_ohlc, timeframe),
                                           major_peaks_n_valleys(multi_timeframe_peaks_n_valleys, timeframe))
        _timeframe_candle_trend['timeframe'] = timeframe
        _timeframe_candle_trend.set_index('timeframe', append=True, inplace=True)
        _timeframe_candle_trend = _timeframe_candle_trend.swaplevel()
        multi_timeframe_candle_trend = pd.concat([multi_timeframe_candle_trend, _timeframe_candle_trend])
    # multi_timeframe_candle_trend = multi_timeframe_candle_trend.sort_index()
    return multi_timeframe_candle_trend
