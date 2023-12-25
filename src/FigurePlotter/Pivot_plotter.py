import pandas as pd
from pandera import typing as pt
from plotly import graph_objects as plgo

from Config import config
from FigurePlotter.OHLVC_plotter import plot_ohlcv
from FigurePlotter.plotter import save_figure, file_id, timeframe_color
from Model.Pivot import MultiTimeframePivot, Pivot
from data_preparation import single_timeframe
from helper import measure_time
from ohlcv import read_multi_timeframe_ohlcv


@measure_time
def plot_multi_timeframe_pivots(multi_timeframe_pivots: pt.DataFrame[MultiTimeframePivot],
                                date_range_str: str = None,
                                name: str = '', show: bool = True,
                                html_path: str = '', save: bool = True) -> plgo.Figure:
    # Create the figure using plot_peaks_n_valleys function
    multi_timeframe_ohlcv = read_multi_timeframe_ohlcv(date_range_str)
    end_time = max(multi_timeframe_ohlcv.index.get_level_values('date'))
    base_ohlcv = single_timeframe(multi_timeframe_ohlcv, config.timeframes[0])
    fig = plot_ohlcv(ohlcv=base_ohlcv, show=False, save=False, name=f'ohlcv{config.timeframes[0]}')
    for timeframe in config.timeframes[1:]:
        ohlcv = single_timeframe(multi_timeframe_ohlcv, timeframe)
        fig.add_trace(plgo.Candlestick(x=ohlcv.index,
                                       open=ohlcv['open'],
                                       high=ohlcv['high'],
                                       low=ohlcv['low'],
                                       close=ohlcv['close'], name=f'ohlcv{timeframe}'))

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
        file_name = f'{file_id(base_ohlcv, name)}'
        save_figure(fig, file_name, html_path)

    if show: fig.show()
    return fig
