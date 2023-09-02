from pandera import typing as pt
from plotly import graph_objects as plgo
import pandas as pd

from Candle import read_ohlca
from Config import config
from FigurePlotter.DataPreparation_plotter import plot_ohlca
from FigurePlotter.plotter import save_figure, file_id, timeframe_color
from Model.MultiTimeframePivot import MultiTimeframePivot
from Model.Pivot import Pivot

"""
class Pivot(pandera.DataFrameModel):
    date: pt.Index[datetime]
    movement_start_time: pt.Series[datetime]
    movement_start_value: pt.Series[datetime]
    return_end_time: pt.Series[datetime]
    return_end_value: pt.Series[datetime]
    level: pt.Series[float]
    internal_margin: pt.Series[float]
    external_margin: pt.Series[float]
    is_active: pt.Series[bool]
    hit: pt.Series[int]
    overlapped_with_major_timeframe: pt.Series[bool]


class MultiTimeframePivot(Pivot, MultiTimeframe):
    pass
"""


def plot_multi_timeframe_bull_bear_side_pivots(multi_timeframe_pivots: pt.DataFrame[MultiTimeframePivot],
                                               date_range_string: str = config.under_process_date_range,
                                               name: str = '', show: bool = True,
                                               html_path: str = '', save: bool = True) -> plgo.Figure:
    # Create the figure using plot_peaks_n_valleys function
    base_ohlca = read_ohlca(date_range_string)
    end_time = base_ohlca.index[-1]
    fig = plot_ohlca(ohlca=base_ohlca, show=False, save=False, name=f'ohlca{config.timeframes[0]}')
    multi_timeframe_pivots.sort_index(level='date', inplace=True)
    for (pivot_timeframe, pivot_start), pivot_info in multi_timeframe_pivots.iterrows():
        pivot_name = Pivot.name(pivot_start, pivot_timeframe, pivot_info)
        pivot_description = Pivot.description(pivot_start, pivot_timeframe, pivot_info)
        # add movement and return paths
        fig.add_scatter(x=[
            pivot_info['movement_start_time'], pivot_start, pivot_info['return_end_time']],
            y=[pivot_info['movement_start_value'], pivot_info['level'], pivot_info['return_end_value']],
            name=pivot_name, line=dict(color='cyan', width=0.5), mode='lines',  # +text',
            legendgroup=pivot_name, showlegend=False, hoverinfo='none',
        )

        # add a dotted line from creating time of level to the activation time
        fig.add_scatter(
            x=[pivot_start, pivot_info['activation_time']],
            y=[pivot_info['level'], pivot_info['level']],
            name=pivot_name, line=dict(color='blue', dash='dot', width=0.5), mode='lines',  # +text',
            legendgroup=pivot_name, showlegend=False, hoverinfo='none',
        )
        # draw the level line
        if pd.isnull(pivot_info['deactivation_time']):
            raise Exception(f'Every level should have a deactivation_time bit in {pivot_timeframe}, {pivot_start},'
                            f' {pivot_info} deactivation_time is Null')
        #     level_end_time = end_time
        # else:
        #     level_end_time = pivot_info['deactivation_time']
        level_end_time = min(pivot_info['deactivation_time'], end_time)
        fig.add_scatter(
            x=[pivot_info['activation_time'], level_end_time],
            y=[pivot_info['level'], pivot_info['level']],
            text=[pivot_description] * 2,
            name=pivot_name, line=dict(color='blue', width=0.5), mode='lines',  # +text',
            legendgroup=pivot_name, showlegend=False, hoverinfo='text',
        )
        # draw the level boundary
        fig.add_scatter(
            x=[pivot_info['activation_time'], level_end_time, level_end_time, pivot_info['activation_time']],
            y=[pivot_info['external_margin'], pivot_info['external_margin'],
               pivot_info['internal_margin'], pivot_info['internal_margin']],
            fill="toself", fillpattern=dict(fgopacity=0.85),
            name=pivot_name, line=dict(color=timeframe_color(pivot_timeframe), width=0), mode='lines',  # +text',
            legendgroup=pivot_name, # hoverinfo='text', text=pivot_description,
        )
    if save or html_path != '':
        file_name = f'multi_timeframe_bull_bear_side_pivots.{file_id(base_ohlca, name)}'
        save_figure(fig, file_name, html_path)

    if show: fig.show()
    return fig
