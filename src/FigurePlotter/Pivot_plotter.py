import pandas as pd
from pandera import typing as pt
from plotly import graph_objects as plgo

from Config import config
from FigurePlotter.OHLVC_plotter import plot_ohlcv
from FigurePlotter.plotter import timeframe_color, show_and_save_plot
from PanderaDFM.AtrTopPivot import MultiTimeframeAtrMovementPivotDf
from PanderaDFM.Pivot import MultiTimeframePivot, Pivot
from helper.data_preparation import single_timeframe
from helper.helper import measure_time
from ohlcv import read_multi_timeframe_ohlcv


# @measure_time
# def plot_multi_timeframe_atr_movement_pivots(multi_timeframe_pivots: pt.DataFrame[MultiTimeframePivot],
#                                              date_range_str: str = None, show: bool = True,
#                                              save: bool = True) -> plgo.Figure:
#     fig = plot_multi_timeframe_pivots(multi_timeframe_pivots, date_range_str, show=False, save=False)
#     for index, pivot in multi_timeframe_pivots.iterrows():
#         pivot_start = index[MultiTimeAtrMovementPivotDf.index_id('date')]
#         pivot_timeframe = index[MultiTimeAtrMovementPivotDf.index_id('timeframe')]
#         pivot_name = Pivot.name(pivot_start, pivot_timeframe, pivot)
#         pivot_description = Pivot.description(pivot_start, pivot_timeframe, pivot)
#         text = pivot_description
#         legend_group = pivot_name
#         # add movement start scatter
#         fig.add_scatter(x=[index[MultiTimeAtrMovementPivotDf.index_id('date')], pivot['movement_start_time']],
#                         y=[pivot['level'], pivot['movement_start_value'], ],  # mode="line",
#                         fillcolor="yellow",
#                         name="NoT",
#                         showlegend=False,
#                         text=[text],
#                         hovertemplate="%{text}",
#                         legendgroup=legend_group, )
#
#         # add return end scatter
#         fig.add_scatter(x=[index[MultiTimeAtrMovementPivotDf.index_id('date')], pivot['return_end_time']],
#                         y=[pivot['level'], pivot['return_end_value'], ],  # mode="line",
#                         fillcolor="yellow",
#                         name="NoT",
#                         showlegend=False,
#                         text=[text],
#                         hovertemplate="%{text}",
#                         legendgroup=legend_group, )
#
#     show_and_save_plot(fig, save, show, name_without_prefix=f'multi_timeframe_classic_pivots.{date_range_str}')
#     return fig


@measure_time
def plot_multi_timeframe_pivots(multi_timeframe_pivots: pt.DataFrame[MultiTimeframePivot],
                                date_range_str: str = None, show: bool = True, save: bool = True) -> plgo.Figure:
    if date_range_str is None:
        date_range_str = config.processing_date_range
    # Create the figure using plot_peaks_n_valleys function
    multi_timeframe_ohlcv = read_multi_timeframe_ohlcv(date_range_str)
    end_time = max(multi_timeframe_ohlcv.index.get_level_values('date'))
    base_ohlcv = single_timeframe(multi_timeframe_ohlcv, config.timeframes[0])
    # plot multiple timeframes candle chart all together.
    fig = plot_ohlcv(ohlcv=base_ohlcv, show=False, save=False, name=config.timeframes[0])
    for timeframe in config.timeframes[1:]:
        ohlcv = single_timeframe(multi_timeframe_ohlcv, timeframe)
        fig.add_trace(plgo.Candlestick(x=ohlcv.index,
                                       open=ohlcv['open'],
                                       high=ohlcv['high'],
                                       low=ohlcv['low'],
                                       close=ohlcv['close'], name=timeframe))

    multi_timeframe_pivots = multi_timeframe_pivots.sort_index(level='date')
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
                name=pivot_name, line=dict(color='red', width=0.5), mode='lines',  # +text',
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
        color = 'orange' if pivot_info['is_resistance'] else 'magenta'
        if pd.isnull(pivot_info['ttl']):
            raise Exception(f'Every level should have a ttl bit in {pivot_timeframe}, {pivot_start},'
                            f' {pivot_info} ttl is Null')
        level_end_time = min(pivot_info['ttl'], end_time)
        fig.add_scatter(
            x=[pivot_info['activation_time'], level_end_time],
            y=[pivot_info['level'], pivot_info['level']],
            text=[pivot_description] * 2,
            name=pivot_name, line=dict(color=color, width=0.5), mode='lines',  # +text',
            legendgroup=pivot_name, showlegend=True, hoverinfo='text',
        )
        # # draw the level boundary
        # fig.add_scatter(
        #     x=[pivot_info['activation_time'], level_end_time, level_end_time, pivot_info['activation_time']],
        #     y=[pivot_info['external_margin'], pivot_info['external_margin'],
        #        pivot_info['internal_margin'], pivot_info['internal_margin']],
        #     fill="toself", fillpattern=dict(fgopacity=0.85),
        #     name=pivot_name, line=dict(color=timeframe_color(pivot_timeframe), width=0), mode='lines',  # +text',
        #     legendgroup=pivot_name, showlegend=False, # hoverinfo='text', text=pivot_description,
        # )
    show_and_save_plot(fig, save, show, name_without_prefix=f'multi_timeframe_classic_pivots.{date_range_str}')
    return fig
