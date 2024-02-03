import pandas as pd
from pandera import typing as pt
from plotly import graph_objects as plgo

from Config import config
from FigurePlotter.OHLVC_plotter import plot_ohlcv, add_atr_scatter
from FigurePlotter.plotter import show_and_save_plot
from PanderaDFM.Pivot import MultiTimeframePivotDFM, PivotDFM
from atr import read_multi_timeframe_ohlcva
from helper.data_preparation import single_timeframe
from helper.helper import measure_time


@measure_time
def plot_multi_timeframe_pivots(multi_timeframe_pivots: pt.DataFrame[MultiTimeframePivotDFM],
                                date_range_str: str = None, show: bool = True, save: bool = True) -> plgo.Figure:
    if date_range_str is None:
        date_range_str = config.processing_date_range
    # Create the figure using plot_peaks_n_valleys function
    multi_timeframe_ohlcva = read_multi_timeframe_ohlcva(date_range_str)
    end_time = max(multi_timeframe_ohlcva.index.get_level_values('date'))
    base_ohlcv = single_timeframe(multi_timeframe_ohlcva, config.timeframes[0])

    # plot multiple timeframes candle chart all together.
    resistance_timeframes = multi_timeframe_pivots[multi_timeframe_pivots['is_resistance']] \
        .index.get_level_values(level='timeframe').unique()
    support_timeframes = multi_timeframe_pivots[~multi_timeframe_pivots['is_resistance']] \
        .index.get_level_values(level='timeframe').unique()
    fig = plot_ohlcv(ohlcv=base_ohlcv, show=False, save=False, name=config.timeframes[0])
    for timeframe in config.timeframes[1:]:
        ohlcva = single_timeframe(multi_timeframe_ohlcva, timeframe)
        fig.add_trace(plgo.Candlestick(x=ohlcva.index,
                                       open=ohlcva['open'],
                                       high=ohlcva['high'],
                                       low=ohlcva['low'],
                                       close=ohlcva['close'], name=timeframe, legendgroup=timeframe))
        midpoints = (ohlcva['high'] + ohlcva['low']) / 2
        # Add the atr boundaries
        fig = add_atr_scatter(fig, ohlcva.index, midpoints=midpoints,
                              widths=ohlcva['atr'],
                              name=timeframe, legendgroup=timeframe, showlegend=False)
        if timeframe in resistance_timeframes:
            pivot_name = f"Piv{timeframe}R"
            fig.add_scatter(x=[base_ohlcv.index[0]], y=[base_ohlcv['open']],
                            name=pivot_name, line=dict(color='red', width=0), mode='lines',
                            legendgroup=pivot_name, showlegend=True, hoverinfo='none',
                            )
        if timeframe in support_timeframes:
            pivot_name = f"Piv{timeframe}S"
            fig.add_scatter(x=[base_ohlcv.index[0]], y=[base_ohlcv['open']],
                            name=pivot_name, line=dict(color='red', width=0), mode='lines',
                            legendgroup=pivot_name, showlegend=True, hoverinfo='none',
                            )

    multi_timeframe_pivots = multi_timeframe_pivots.sort_index(level='date')
    for (pivot_timeframe, pivot_start, pivot_original_start), pivot_info in multi_timeframe_pivots.iterrows():
        pivot_name = f"Piv{pivot_timeframe}R" if pivot_info['is_resistance'] else f"Piv{pivot_timeframe}S"
        pivot_description = PivotDFM.description(pivot_start, pivot_timeframe, pivot_info)
        # add movement and return paths
        if (hasattr(pivot_info, 'movement_start_time')
                and hasattr(pivot_info, 'return_end_time')
                and hasattr(pivot_info, 'movement_start_value')
                and hasattr(pivot_info, 'return_end_value')
        ):
            fig.add_scatter(x=[pivot_info['movement_start_time'], pivot_start, ],
                            y=[pivot_info['movement_start_value'], pivot_info['level'], ],
                            name=pivot_name, line=dict(color='green', width=0.5), mode='lines',  # +text',
                            legendgroup=pivot_name, showlegend=False, hoverinfo='none',
                            )
            fig.add_scatter(x=[pivot_start, pivot_info['return_end_time']],
                            y=[pivot_info['level'], pivot_info['return_end_value']],
                            name=pivot_name, line=dict(color='red', width=0.5), mode='lines',  # +text',
                            legendgroup=pivot_name, showlegend=False, hoverinfo='none',
                            )
        # add a dotted line from creating time of level to the activation time
        fig.add_scatter(
            x=[pivot_start, pivot_original_start],
            y=[pivot_info['level'], pivot_info['level']],
            name=pivot_name, line=dict(color='blue', dash='dot', width=0.5), mode='lines',  # +text',
            legendgroup=pivot_name, showlegend=False, hoverinfo='none',
        )
        # draw the level line
        level_color = 'orange' if pivot_info['is_resistance'] else 'magenta'
        if pd.isnull(pivot_info['ttl']):
            raise Exception(f'Every level should have a ttl.'
                            f'{pivot_timeframe}, {pivot_start}, {pivot_info} ttl is Null')
        level_end_time = min(pivot_info['ttl'], end_time)
        if pd.notna(pivot_info['deactivated_at']):
            level_end_time = min(pivot_info['deactivated_at'], level_end_time)
        fig.add_scatter(
            x=[pivot_start, level_end_time],
            y=[pivot_info['level'], pivot_info['level']],
            text=[pivot_description],
            name=pivot_name, line=dict(color=level_color, width=0.5), mode='lines',  # +text',
            legendgroup=pivot_name, showlegend=False, hoverinfo='text',
        )
        # draw the level boundary
        boundary_color = 'blue' if pivot_info['is_resistance'] else 'red'
        fig.add_scatter(
            x=[pivot_start, level_end_time, level_end_time, pivot_start],
            y=[pivot_info['external_margin'], pivot_info['external_margin'],
               pivot_info['internal_margin'], pivot_info['internal_margin']],
            fill="toself", opacity=0.3,
            name=pivot_name, line=dict(color=boundary_color, width=0), mode='lines',  # +text',
            legendgroup=pivot_name, showlegend=False, hoverinfo='none',  # text=pivot_description,
        )
    show_and_save_plot(fig, save, show, name_without_prefix=f'multi_timeframe_classic_pivots.{date_range_str}')
    return fig
