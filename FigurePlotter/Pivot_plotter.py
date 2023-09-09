import pandas as pd
from pandera import typing as pt
from plotly import graph_objects as plgo

from Candle import read_multi_timeframe_ohlc
from Config import config
from DataPreparation import single_timeframe
from FigurePlotter.DataPreparation_plotter import plot_ohlc
from FigurePlotter.plotter import save_figure, file_id, timeframe_color
from Model.MultiTimeframePivot import MultiTimeframePivot
from Model.Pivot import Pivot
from helper import measure_time

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
    is_overlap_of: pt.Series[bool]


class MultiTimeframePivot(Pivot, MultiTimeframe):
    pass
"""


@measure_time
def plot_multi_timeframe_pivots(multi_timeframe_pivots: pt.DataFrame[MultiTimeframePivot],
                                date_range_string: str = config.under_process_date_range,
                                name: str = '', show: bool = True,
                                html_path: str = '', save: bool = True) -> plgo.Figure:
    # Create the figure using plot_peaks_n_valleys function
    multi_timeframe_ohlc = read_multi_timeframe_ohlc(date_range_string)
    end_time = max(multi_timeframe_ohlc.index.get_level_values('date'))
    base_ohlc = single_timeframe(multi_timeframe_ohlc, config.timeframes[0])
    fig = plot_ohlc(ohlc=base_ohlc, show=False, save=False, name=f'ohlc{config.timeframes[0]}')
    for timeframe in config.timeframes[1:]:
        ohlc = single_timeframe(multi_timeframe_ohlc, timeframe)
        fig.add_trace(plgo.Candlestick(x=ohlc.index,
                                       open=ohlc['open'],
                                       high=ohlc['high'],
                                       low=ohlc['low'],
                                       close=ohlc['close'], name=f'ohlc{timeframe}'))

    multi_timeframe_pivots.sort_index(level='date', inplace=True)
    for (pivot_timeframe, pivot_start), pivot_info in multi_timeframe_pivots.iterrows():
        pivot_name = Pivot.name(pivot_start, pivot_timeframe, pivot_info)
        pivot_description = Pivot.description(pivot_start, pivot_timeframe, pivot_info)
        # add movement and return paths
        if (hasattr(pivot_info, 'movement_start_time')
                and hasattr(pivot_info, 'return_end_time')
                and hasattr(pivot_info, 'movement_start_value')
                and hasattr(pivot_info, 'return_end_value')
        ):
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
        if pd.isnull(pivot_info['ttl']):
            raise Exception(f'Every level should have a ttl bit in {pivot_timeframe}, {pivot_start},'
                            f' {pivot_info} ttl is Null')
        level_end_time = min(pivot_info['ttl'], end_time)
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
            legendgroup=pivot_name,  # hoverinfo='text', text=pivot_description,
        )
    if save or html_path != '':
        file_name = f'{file_id(base_ohlc, name)}'
        save_figure(fig, file_name, html_path)

    if show: fig.show()
    return fig
