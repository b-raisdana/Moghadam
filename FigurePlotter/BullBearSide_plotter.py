from datetime import timedelta
from typing import List

import pandas as pd
from pandas import Timestamp
from plotly import graph_objects as plgo

from Config import TREND, config
from DataPreparation import single_timeframe
from FigurePlotter.plotter import file_id, save_figure, plot_multiple_figures
from PeakValley import plot_peaks_n_valleys, peaks_only, valleys_only, major_peaks_n_valleys

MAX_NUMBER_OF_PLOT_SCATTERS = 1000
def plot_single_timeframe_bull_bear_side_trends(single_timeframe_ohlca: pd.DataFrame, peaks_n_valleys: pd.DataFrame,
                                                boundaries: pd.DataFrame,
                                                name: str = '', show: bool = True,
                                                html_path: str = '', save: bool = True) -> plgo.Figure:
    fig = plot_peaks_n_valleys(single_timeframe_ohlca, peaks=peaks_only(peaks_n_valleys),
                               valleys=valleys_only(peaks_n_valleys), file_name=name, show=False, save=False)
    remained_number_of_scatters = MAX_NUMBER_OF_PLOT_SCATTERS
    for boundary_index, boundary in boundaries.iterrows():
        if boundary_index == Timestamp('2017-01-04 11:17:00'):
            pass
        boundary_peaks_n_valleys = boundary_including_peaks_valleys(peaks_n_valleys, boundary_index,
                                                                    boundary['end'])
        boundary_indexes = single_timeframe_ohlca.loc[boundary_index: boundary['end']].index
        boundary_peaks = peaks_only(boundary_peaks_n_valleys)['high'].sort_index(level='date')
        boundary_valleys = valleys_only(boundary_peaks_n_valleys)['low'].sort_index(level='date', ascending=False)
        xs = [boundary_index] + boundary_peaks.index.get_level_values('date').tolist() + \
             [boundary['end']] + boundary_valleys.index.get_level_values('date').tolist()
        ys = [single_timeframe_ohlca.loc[boundary_index, 'open']] + boundary_peaks.values.tolist() + \
             [single_timeframe_ohlca.loc[boundary['end'], 'close']] + boundary_valleys.values.tolist()
        fill_color = 'green' if boundary['bull_bear_side'] == TREND.BULLISH.value else \
            'red' if boundary['bull_bear_side'] == TREND.BEARISH.value else 'gray'
        text = f'{boundary["bull_bear_side"].replace("_TREND", "")}: ' \
               f'{boundary_index.strftime("%H:%M")}-{boundary["end"].strftime("%H:%M")}:'
        if 'movement' in boundaries.columns.tolist():
            text += f'\nM:{boundary["movement"]:.2f}'
        if 'duration' in boundaries.columns.tolist():
            text += f'D:{boundary["duration"] / timedelta(hours=1):.2f}h'
        if 'strength' in boundaries.columns.tolist():
            text += f'S:{boundary["strength"]:.2f}'
        if 'ATR' in boundaries.columns.tolist():
            text += f'ATR:{boundary["ATR"]:.2f}'
        if remained_number_of_scatters > 0:
            fig.add_scatter(x=xs, y=ys, fill="toself",  # fillcolor=fill_color,
                            fillpattern=dict(fgopacity=0.5, shape='.'),
                            name=f'{boundary["bull_bear_side"].replace("_TREND", "")}: '
                                 f'{boundary_index.strftime("%H:%M")}-{boundary["end"].strftime("%H:%M")}',
                            line=dict(color=fill_color),
                            mode='lines',  # +text',
                            )
            fig.add_scatter(x=boundary_indexes, y=single_timeframe_ohlca.loc[boundary_indexes, 'open'],
                            mode='none',
                            showlegend=False,
                            text=text,
                            hoverinfo='text')

            remained_number_of_scatters -= 1
        else:
            break

    if save or html_path != '':
        file_name = f'single_timeframe_bull_bear_side_trends.{file_id(single_timeframe_ohlca, name)}'
        save_figure(fig, file_name, html_path)

    if show: fig.show()
    return fig


def plot_multi_timeframe_bull_bear_side_trends(multi_timeframe_ohlca, multi_timeframe_peaks_n_valleys,
                                               _multi_timeframe_bull_bear_side_trends, show: bool = True, save: bool = True,
                                               timeframe_shortlist: List['str'] = None):
    figures = []
    if timeframe_shortlist is None:
        timeframe_shortlist = config.timeframes
    for timeframe in timeframe_shortlist:
        _figure = plot_single_timeframe_bull_bear_side_trends(
            single_timeframe_ohlca=single_timeframe(multi_timeframe_ohlca, timeframe),
            peaks_n_valleys=major_peaks_n_valleys(multi_timeframe_peaks_n_valleys, timeframe).sort_index(level='date'),
            boundaries=single_timeframe(_multi_timeframe_bull_bear_side_trends, timeframe).sort_index(level='date'),
            show=False, save=False,
            name=f'{timeframe} boundaries')
        figures.append(_figure)
    plot_multiple_figures(figures, name=f'multi_timeframe_bull_bear_side_trends.'
                                        f'{multi_timeframe_ohlca.index[0][1].strftime("%y-%m-%d.%H-%M")}T'
                                        f'{multi_timeframe_ohlca.index[-1][1].strftime("%y-%m-%d.%H-%M")}',
                          show=show, save=save)


def boundary_including_peaks_valleys(peaks_n_valleys: pd.DataFrame, boundary_start: pd.Timestamp,
                                     boundary_end: pd.Timestamp):
    return peaks_n_valleys.loc[(peaks_n_valleys.index.get_level_values('date') >= boundary_start) &
                               (peaks_n_valleys.index.get_level_values('date') <= boundary_end)]
