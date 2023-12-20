from typing import List

import pandas as pd
from plotly import graph_objects as plgo

import PeakValley
from Config import TREND, config
from FigurePlotter.PeakValley_plotter import plot_peaks_n_valleys
from FigurePlotter.plotter import file_id, save_figure, plot_multiple_figures
from Model.BullBearSide import BullBearSide, bull_bear_side_repr
from Model.OHLCV import OHLCV
from PeakValley import peaks_only, valleys_only, major_peaks_n_valleys
from data_preparation import single_timeframe
from helper import measure_time, log
from pandera import typing as pt
MAX_NUMBER_OF_PLOT_SCATTERS = 5000


def plot_single_timeframe_bull_bear_side_trends(single_timeframe_ohlcva: pt.DataFrame[OHLCV],
                                                peaks_n_valleys: pt.DataFrame[PeakValley],
                                                boundaries: pt.DataFrame[BullBearSide],
                                                name: str = '', show: bool = True,
                                                html_path: str = '', save: bool = True) -> plgo.Figure:
    fig = plot_peaks_n_valleys(single_timeframe_ohlcva, peaks=peaks_only(peaks_n_valleys),
                               valleys=valleys_only(peaks_n_valleys), name=name, show=False, save=False)
    remained_number_of_scatters = MAX_NUMBER_OF_PLOT_SCATTERS
    for _start, _trend in boundaries.iterrows():
        trend_peaks_n_valleys = peaks_valleys_in_range(peaks_n_valleys, _start, _trend['end'])
        included_candles = single_timeframe_ohlcva.loc[_start: _trend['end']].index
        trend_peaks = peaks_only(trend_peaks_n_valleys)['high'].sort_index(level='date')
        trend_valleys = valleys_only(trend_peaks_n_valleys)['low'].sort_index(level='date', ascending=False)
        xs = [_start] + trend_peaks.index.get_level_values('date').tolist() + \
             [_trend['end']] + trend_valleys.index.get_level_values('date').tolist()[::-1]
        ys = [single_timeframe_ohlcva.loc[_start, 'open']] + trend_peaks.values.tolist() + \
             [single_timeframe_ohlcva.loc[_trend['end'], 'close']] + trend_valleys.values.tolist()[::-1]
        fill_color = 'green' if _trend['bull_bear_side'] == TREND.BULLISH.value else \
            'red' if _trend['bull_bear_side'] == TREND.BEARISH.value else 'gray'
        text = bull_bear_side_repr(_start, _trend, boundaries)
        if remained_number_of_scatters > 0:
            fig.add_scatter(x=xs, y=ys, fill="toself",  # fillcolor=fill_color,
                            fillpattern=dict(fgopacity=0.5, shape='.'),
                            name=f'{_trend["bull_bear_side"].replace("_TREND", "")}: '
                                 f'{_start.strftime("%H:%M")}-{_trend["end"].strftime("%H:%M")}',
                            line=dict(color=fill_color, width=0),
                            mode='lines',  # +text',
                            legendgroup=f'{_trend["bull_bear_side"].replace("_TREND", "")}: '
                                        f'{_start.strftime("%H:%M")}-{_trend["end"].strftime("%H:%M")}',
                            )
            fig.add_scatter(x=included_candles, y=single_timeframe_ohlcva.loc[included_candles, 'open'],
                            mode='none',
                            showlegend=False,
                            text=text,
                            legendgroup=f'{_trend["bull_bear_side"].replace("_TREND", "")}: '
                                        f'{_start.strftime("%H:%M")}-{_trend["end"].strftime("%H:%M")}',
                            hoverinfo='text')

            if 'movement_start_value' in boundaries.columns:
                fig.add_scatter(x=[_trend['movement_start_time'], _trend['movement_end_time']],
                                y=[_trend['movement_start_value'], _trend['movement_end_value']],
                                line=dict(color=fill_color),
                                legendgroup=f'{_trend["bull_bear_side"].replace("_TREND", "")}: '
                                            f'{_start.strftime("%H:%M")}-{_trend["end"].strftime("%H:%M")}',
                                )
            else:
                log(f'movement not found in boundaries:{boundaries.columns}', stack_trace=False)
            remained_number_of_scatters -= 2
        else:
            break

    if save or html_path != '':
        file_name = f'single_timeframe_bull_bear_side_trends.{file_id(single_timeframe_ohlcva, name)}'
        save_figure(fig, file_name, html_path)

    if show: fig.show()
    return fig


@measure_time
def plot_multi_timeframe_bull_bear_side_trends(multi_timeframe_ohlcva, multi_timeframe_peaks_n_valleys,
                                               _multi_timeframe_bull_bear_side_trends, show: bool = True,
                                               save: bool = True,
                                               timeframe_shortlist: List['str'] = None):
    figures = []
    if timeframe_shortlist is None:
        timeframe_shortlist = config.timeframes
    for timeframe in timeframe_shortlist:
        _figure = plot_single_timeframe_bull_bear_side_trends(
            single_timeframe_ohlcva=single_timeframe(multi_timeframe_ohlcva, timeframe),
            peaks_n_valleys=major_peaks_n_valleys(multi_timeframe_peaks_n_valleys, timeframe).sort_index(level='date'),
            boundaries=single_timeframe(_multi_timeframe_bull_bear_side_trends, timeframe).sort_index(level='date'),
            show=False, save=False,
            name=f'{timeframe} boundaries')
        figures.append(_figure)
    plot_multiple_figures(figures, name=f'multi_timeframe_bull_bear_side_trends.'
                                        f'{multi_timeframe_ohlcva.index[0][1].strftime("%y-%m-%d.%H-%M")}T'
                                        f'{multi_timeframe_ohlcva.index[-1][1].strftime("%y-%m-%d.%H-%M")}',
                          show=show, save=save)


def peaks_valleys_in_range(peaks_n_valleys: pd.DataFrame, start: pd.Timestamp, end: pd.Timestamp):
    return peaks_n_valleys.loc[(peaks_n_valleys.index.get_level_values('date') >= start) &
                               (peaks_n_valleys.index.get_level_values('date') <= end)]
