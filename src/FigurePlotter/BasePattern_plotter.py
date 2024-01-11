import os
import webbrowser
from datetime import datetime
from typing import List, Union

import backtrader as bt
import numpy as np
import pandas as pd
import plotly.graph_objs
from pandera import typing as pt
from plotly import graph_objects as plgo

from BasePattern import timeframe_effective_bases
from Config import config
from FigurePlotter.OHLVC_plotter import plot_ohlcva, plot_merged_timeframe_ohlcva
from FigurePlotter.plotter import file_id, save_figure, plot_multiple_figures
from Model.Order import OrderSide
from PanderaDFM.BasePattern import BasePattern, MultiTimeframeBasePattern
from PanderaDFM.OHLCV import OHLCV
from PanderaDFM.OHLCVA import MultiTimeframeOHLCVA
from helper.data_preparation import single_timeframe
from helper.helper import measure_time

MAX_NUMBER_OF_PLOT_SCATTERS = 5000


def plot_single_timeframe_base_pattern(single_timeframe_ohlcva: pt.DataFrame[OHLCV],
                                       timeframe_base_patterns: pt.DataFrame[BasePattern],
                                       base_ohlcv: Union[pt.DataFrame[OHLCV] | None],
                                       timeframe: str,
                                       name: str = '', show: bool = True,
                                       html_path: str = '', save: bool = True) -> plgo.Figure:
    if len(single_timeframe_ohlcva) == 0:
        return plot_ohlcva(single_timeframe_ohlcva, name=name, show=False, save=False)
    fig: plgo.Figure = plot_ohlcva(single_timeframe_ohlcva, base_ohlcv=base_ohlcv, name=name, show=False, save=False)
    # remained_number_of_scatters = MAX_NUMBER_OF_PLOT_SCATTERS
    timeframe_base_patterns['effective_end'] = timeframe_base_patterns[['end', 'ttl']].min(axis=1, skipna=True)
    ohlcva_end = single_timeframe_ohlcva.index[-1] + pd.to_timedelta(timeframe)
    timeframe_base_patterns.loc[timeframe_base_patterns['effective_end'] > ohlcva_end, 'effective_end'] = ohlcva_end
    assert timeframe_base_patterns['effective_end'].notna().all()
    for (timeframe, index_date), base_pattern in timeframe_base_patterns.iterrows():
        start = index_date + \
                pd.to_timedelta(timeframe) * config.base_pattern_index_shift_after_last_candle_in_the_sequence
        # mid_level = (base_pattern['internal_high'] + base_pattern['internal_low']) / 2
        xs = [start, base_pattern['effective_end'], base_pattern['effective_end'],
              start]
        ys = [base_pattern['internal_low'], base_pattern['internal_low'], \
              base_pattern['internal_high'], base_pattern['internal_high']]
        fill_color = 'blue'
        name = MultiTimeframeBasePattern.str(index_date, timeframe, base_pattern)
        text = MultiTimeframeBasePattern.repr(index_date, timeframe, base_pattern)
        fig.add_scatter(x=xs, y=ys, fill="toself",  # fillcolor=fill_color,
                        fillpattern=dict(fgopacity=0.5),
                        name=name,
                        text=text,
                        line=dict(color=fill_color, width=0),
                        mode='lines',
                        legendgroup=name,
                        hoverinfo='text'
                        )
        if base_pattern['below_band_activated'] is not None:
            # add a vertical line equal to atr of BasePattern at the time price chart goes 1 atr under below edge.
            xs = [start, base_pattern['below_band_activated']]
            ys = [base_pattern['internal_low'], base_pattern['internal_high'] + base_pattern['atr']]
            fig.add_scatter(x=xs, y=ys,
                            name=name,
                            text=text,
                            line=dict(color='yellow', width=1),
                            mode='lines',  # +text',
                            showlegend=False,
                            legendgroup=name,
                            hovertemplate="%{text}",
                            )
        if base_pattern['upper_band_activated'] is not None:
            # add a vertical line equal to atr of BasePattern at the time price chart goes 1 atr above upper edge.
            xs = [start, base_pattern['upper_band_activated']]
            ys = [base_pattern['internal_high'], base_pattern['internal_low'] - base_pattern['atr']]
            fig.add_scatter(x=xs, y=ys,
                            name=name,
                            text=text,
                            line=dict(color='yellow', width=1),
                            mode='lines',  # +text',
                            showlegend=False,
                            legendgroup=name,
                            hovertemplate="%{text}",
                            )
    fig.update_layout({
        'width': 1800,  # Set the width of the plot
        'height': 900,
        'legend': {
            'font': {
                'size': 8
            }
        }
    })
    # go.Layout(
    #     legend=dict(
    #         font=dict(
    #             size=16  # Set the font size for the legend text
    #         )
    #     )
    # )
    if save or html_path != '':
        file_name = f'single_timeframe_base_pattern.{file_id(single_timeframe_ohlcva, name)}'
        save_figure(fig, file_name, html_path)

    if show: fig.show()
    return fig


@measure_time
def old_plot_multi_timeframe_base_pattern(multi_timeframe_ohlcva: pt.DataFrame[MultiTimeframeOHLCVA],
                                          _multi_timeframe_base_pattern: pt.DataFrame[MultiTimeframeBasePattern],
                                          show: bool = True, save: bool = True, timeframe_shortlist: List['str'] = None) \
        -> None:
    figures = []
    if timeframe_shortlist is None:
        timeframe_shortlist = config.timeframes
    base_ohlcv = single_timeframe(multi_timeframe_ohlcva, config.timeframes[0])
    for timeframe in timeframe_shortlist:
        if timeframe != config.timeframes[0]:
            _figure = plot_single_timeframe_base_pattern(
                single_timeframe(multi_timeframe_ohlcva, timeframe),
                timeframe_effective_bases(_multi_timeframe_base_pattern, timeframe).copy(),
                base_ohlcv=base_ohlcv, timeframe=timeframe,
                show=False, save=False, name=f'{timeframe} boundaries')
        else:
            _figure = plot_single_timeframe_base_pattern(
                single_timeframe(multi_timeframe_ohlcva, timeframe),
                timeframe_effective_bases(_multi_timeframe_base_pattern, timeframe).copy(),
                base_ohlcv=None, timeframe=timeframe,
                show=False, save=False, name=f'{timeframe} boundaries')
        figures.append(_figure)
    plot_multiple_figures(figures, name=f'multi_timeframe_base_pattern.'
                                        f'{multi_timeframe_ohlcva.index[0][1].strftime("%y-%m-%d.%H-%M")}T'
                                        f'{multi_timeframe_ohlcva.index[-1][1].strftime("%y-%m-%d.%H-%M")}',
                          show=show, save=save)


@measure_time
def plot_multi_timeframe_base_pattern(_multi_timeframe_base_pattern: pt.DataFrame[MultiTimeframeBasePattern],
                                      _multi_timeframe_ohlcva: pt.DataFrame[MultiTimeframeOHLCVA],
                                      order_groups: List[tuple[bt.Order, bt.Order, bt.Order]] = None,
                                      show: bool = True, save: bool = True) \
        -> None:
    fig: plgo.Figure = plot_merged_timeframe_ohlcva(_multi_timeframe_ohlcva)
    ohlcva_end = _multi_timeframe_ohlcva.index.get_level_values('date').max()
    _multi_timeframe_base_pattern['effective_end'] = \
        _multi_timeframe_base_pattern[['end', 'ttl']].min(axis=1, skipna=True)
    _multi_timeframe_base_pattern.loc[_multi_timeframe_base_pattern['effective_end'] > ohlcva_end, 'effective_end'] = \
        ohlcva_end
    assert _multi_timeframe_base_pattern['effective_end'].notna().all()
    for (timeframe, index_date), base_pattern in _multi_timeframe_base_pattern.iterrows():
        start = index_date + \
                pd.to_timedelta(timeframe) * config.base_pattern_index_shift_after_last_candle_in_the_sequence
        name, text = draw_base(base_pattern, fig, index_date, start, timeframe)
        draw_band_activators(base_pattern, fig, name, start, text)
        if order_groups is not None:
            draw_band_orders(order_groups, fig, name, start, text)
    if save:
        file_name = f'merged_base_pattern.{file_id(_multi_timeframe_ohlcva)}'
        save_figure(fig, file_name)

    if show: fig.show()

    show_and_save_plot(fig, save, show, name=f'merged_base_pattern.{file_id(_multi_timeframe_ohlcva)}')
    return fig


def show_and_save_plot(fig: plotly.graph_objs.Figure, save: bool, show: bool, name:str, path_of_directory: str = None):
    if path_of_directory is None:
        path_of_directory = config.path_of_plots
    file_path = os.path.join(path_of_directory, f'{name}.html')
    with open(file_path, "w", encoding="utf-8") as file:
        file.write(fig.to_html())
    if show:
        full_path = os.path.abspath(file_path)
        webbrowser.register('firefox',
                            None,
                            webbrowser.BackgroundBrowser("C://Program Files//Mozilla Firefox//firefox.exe"))
        webbrowser.get('firefox').open(f'file://{full_path}')
        # display(combined_html, raw=True, clear=True)  # Show the final HTML in the browser
    if not save:
        os.remove(file_path)


def draw_band_activators(base_pattern, fig, name, start, text):
    if base_pattern['below_band_activated'] is not None:
        # add a vertical line equal to atr of BasePattern at the time price chart goes 1 atr under below edge.
        xs = [start, base_pattern['below_band_activated']]
        ys = [base_pattern['internal_low'], base_pattern['internal_high'] + base_pattern['atr']]
        fig.add_scatter(x=xs, y=ys,
                        name=name,
                        text=text,
                        line=dict(color='yellow', width=1),
                        mode='lines',  # +text',
                        showlegend=False,
                        legendgroup=name,
                        hovertemplate="%{text}",
                        )
    if base_pattern['upper_band_activated'] is not None:
        # add a vertical line equal to atr of BasePattern at the time price chart goes 1 atr above upper edge.
        xs = [start, base_pattern['upper_band_activated']]
        ys = [base_pattern['internal_high'], base_pattern['internal_low'] - base_pattern['atr']]
        fig.add_scatter(x=xs, y=ys,
                        name=name,
                        text=text,
                        line=dict(color='yellow', width=1),
                        mode='lines',  # +text',
                        showlegend=False,
                        legendgroup=name,
                        hovertemplate="%{text}",
                        )


def draw_band_orders(order_groups: List[tuple[bt.Order, bt.Order, bt.Order]], fig: plotly.graph_objs.Figure,
                     start: datetime, legend_group: str, end_of_dates: datetime, end_of_prices: float):
    for index in range(len(order_groups)):
        original_ordr: bt.Order
        stop_loss_ordr: bt.Order
        take_profit_ordr: bt.Order
        original_ordr, stop_loss_ordr, take_profit_ordr = order_groups[index]
        start_date = bt.num2date(original_ordr.created.dt)
        start_price = original_ordr.created.price
        stop_loss_price = stop_loss_ordr.created.price
        take_profit_price = take_profit_ordr.created.price
        end = max(stop_loss_ordr.executed.dt, take_profit_price.executed.dt)
        final_price = max(stop_loss_ordr.executed.price, take_profit_price.executed.price)
        if final_price == 0:
            final_price = end_of_prices
        side = OrderSide.Buy if original_ordr.size > 0 else OrderSide.Sell
        if end == 0:
            end = end_of_dates
            final_price = np.Inf
        if side == OrderSide.Buy:
            pnl_value = final_price - start_price
        else:  # order is Sell
            pnl_value = start_price - final_price
        pnl_percent = pnl_value / abs(start_price - stop_loss_price)
        text = (f"{side.value.upper()}@{start_date.strftime('%m/%d-%H:%M')}\r\n"
                f"{start_price}"
                f"SL{stop_loss_price}"
                f"TP{take_profit_price}"
                f"PNL{pnl_value}/{pnl_percent}"
                f"Base={legend_group}"
                )
        # draw stop-loss box
        xs = [start, end, end, start]
        ys = [start_price, start_price, stop_loss_price, stop_loss_price]
        fig.add_scatter(xs, ys, fill="toself",  # fillcolor=fill_color,
                        fillpattern=dict(fgopacity=0.5), text=text,
                        line=dict(color='red', width=0),
                        mode='lines',
                        legendgroup=legend_group,
                        hoverinfo='text'
                        )
        # draw take_profit box
        xs = [start, end, end, start]
        ys = [start_price, start_price, take_profit_price, take_profit_price]
        fig.add_scatter(xs, ys, fill="toself",  # fillcolor=fill_color,
                        fillpattern=dict(fgopacity=0.5), text=text,
                        line=dict(color='green', width=0),
                        mode='lines',
                        legendgroup=legend_group,
                        hoverinfo='text'
                        )
        # draw pnl lien
        line_color = 'green' if pnl_value > 0 else 'red'
        xs = [start, end]
        ys = [start_price, final_price]
        fig.add_scatter(xs, ys, fill="toself",  # fillcolor=fill_color,
                        fillpattern=dict(fgopacity=0.5), text=text,
                        line=dict(color='green', width=1),
                        mode='lines',
                        legendgroup=legend_group,
                        # hoverinfo='text'
                        )


def draw_base(base_pattern, fig, index_date, start, timeframe):
    xs = [start, base_pattern['effective_end'], base_pattern['effective_end'], start]
    ys = [base_pattern['internal_low'], base_pattern['internal_low'], \
          base_pattern['internal_high'], base_pattern['internal_high']]
    fill_color = 'blue'
    name = MultiTimeframeBasePattern.str(index_date, timeframe, base_pattern)
    text = MultiTimeframeBasePattern.repr(index_date, timeframe, base_pattern)
    fig.add_scatter(x=xs, y=ys, fill="toself",  # fillcolor=fill_color,
                    fillpattern=dict(fgopacity=0.5),
                    name=name,
                    text=text,
                    line=dict(color=fill_color, width=0),
                    mode='lines',
                    legendgroup=name,
                    hoverinfo='text'
                    )
    return name, text
