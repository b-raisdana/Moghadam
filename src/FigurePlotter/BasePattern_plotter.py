import os
import webbrowser
from datetime import datetime

import pandas as pd
import plotly.graph_objs
from pandera import typing as pt
from plotly import graph_objects as plgo

from Config import config
from FigurePlotter.OHLVC_plotter import plot_merged_timeframe_ohlcva
from FigurePlotter.plotter import file_id
from Model.Order import OrderSide, BracketOrderType
from PanderaDFM.BasePattern import MultiTimeframeBasePattern
from PanderaDFM.OHLCVA import MultiTimeframeOHLCVA
from helper.helper import measure_time

MAX_NUMBER_OF_PLOT_SCATTERS = 5000


@measure_time
def plot_multi_timeframe_base_pattern(_multi_timeframe_base_pattern: pt.DataFrame[MultiTimeframeBasePattern],
                                      _multi_timeframe_ohlcva: pt.DataFrame[MultiTimeframeOHLCVA],
                                      # order_groups: dict[int, tuple[bt.Order, bt.Order, bt.Order]] = None,
                                      orders_df: pd.DataFrame,
                                      show: bool = True, save: bool = True) \
        -> None:
    fig: plgo.Figure = plot_merged_timeframe_ohlcva(_multi_timeframe_ohlcva)
    _multi_timeframe_ohlcva = _multi_timeframe_ohlcva.sort_index(level='date')
    end_of_dates = _multi_timeframe_ohlcva.index.get_level_values('date').max()
    end_of_prices = _multi_timeframe_ohlcva.loc[(config.timeframes[0], end_of_dates)]['close']
    _multi_timeframe_base_pattern['effective_end'] = \
        _multi_timeframe_base_pattern[['end', 'ttl']].min(axis=1, skipna=True)
    _multi_timeframe_base_pattern.loc[_multi_timeframe_base_pattern['effective_end'] > end_of_dates, 'effective_end'] = \
        end_of_dates
    assert _multi_timeframe_base_pattern['effective_end'].notna().all()
    for (timeframe, index_date), base_pattern in _multi_timeframe_base_pattern.iterrows():
        if timeframe in config.timeframes[2:-2]:
            start = index_date + \
                    pd.to_timedelta(timeframe) * config.base_pattern_index_shift_after_last_candle_in_the_sequence
            legend_group, text = draw_base(base_pattern, fig, index_date, start, timeframe)
            draw_band_activators(base_pattern, fig, legend_group, start, text)
            # if order_groups is not None:
            #     draw_band_order_groups(order_groups, fig, end_of_dates, end_of_prices,
            #                            index_date, timeframe, legend_group)
            if not orders_df.empty:
                draw_band_orders_df(orders_df, fig, end_of_dates, end_of_prices,
                                    index_date, timeframe, legend_group)
    show_and_save_plot(fig, save, show, name=f'merged_base_pattern.{file_id(_multi_timeframe_ohlcva)}')


def show_and_save_plot(fig: plotly.graph_objs.Figure, save: bool, show: bool, name: str, path_of_directory: str = None):
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


def draw_band_activators(base_pattern, fig, legend_group, start, text):
    if base_pattern['below_band_activated'] is not None:
        # add a vertical line equal to atr of BasePattern at the time price chart goes 1 atr under below edge.
        xs = [start, base_pattern['below_band_activated']]
        ys = [base_pattern['internal_low'], base_pattern['internal_high'] + base_pattern['atr']]
        fig.add_scatter(x=xs, y=ys,
                        name=legend_group,
                        text=text,
                        line=dict(color='yellow', width=1),
                        mode='lines',  # +text',
                        showlegend=False,
                        legendgroup=legend_group,
                        hovertemplate="%{text}",
                        )
    if base_pattern['upper_band_activated'] is not None:
        # add a vertical line equal to atr of BasePattern at the time price chart goes 1 atr above upper edge.
        xs = [start, base_pattern['upper_band_activated']]
        ys = [base_pattern['internal_high'], base_pattern['internal_low'] - base_pattern['atr']]
        fig.add_scatter(x=xs, y=ys,
                        name=legend_group,
                        text=text,
                        line=dict(color='yellow', width=1),
                        mode='lines',  # +text',
                        showlegend=False,
                        legendgroup=legend_group,
                        hovertemplate="%{text}",
                        )


def draw_band_orders_df(orders_df: pd.DataFrame, fig: plotly.graph_objs.Figure,
                        end_of_dates: datetime, end_of_prices: float,
                        base_pattern_date: pd.Timestamp, base_pattern_timeframe: str,
                        legend_group: str):
    # if orders_df['date'].dtype != pd.Timestamp:
    #     orders_df['date'] = datetime.strptime(orders_df['date'], "%Y-%m-%d %H:%M:%S%z")
    # orders_df['date'] = str(orders_df['date'])
    order_group_ids = orders_df.loc[(
            (orders_df['reference_date'] == str(base_pattern_date))
            & (orders_df['reference_timeframe'] == base_pattern_timeframe)
    ), 'info_order_group_id'].unique().tolist()
    for order_group_id in order_group_ids:
        original_order: pd.Series = orders_df[
            (orders_df['info_order_group_id'] == order_group_id)
            & (orders_df['info_custom_type'] == BracketOrderType.Original.value)
            ].sort_index(level='date').iloc[-1].to_dict()
        stop_loss_order: pd.Series = orders_df[
            (orders_df['info_order_group_id'] == order_group_id)
            & (orders_df['info_custom_type'] == BracketOrderType.StopLoss.value)
            ].sort_index(level='date').iloc[-1].to_dict()
        take_profit_order: pd.Series = orders_df[
            (orders_df['info_order_group_id'] == order_group_id)
            & (orders_df['info_custom_type'] == BracketOrderType.TakeProfit.value)
            ].sort_index(level='date').iloc[-1].to_dict()
        if len(original_order) == 0 or len(stop_loss_order) == 0 or len(take_profit_order) == 0:
            AssertionError("original_order.empty or stop_loss_order.empty or take_profit_order.empty")
        start = datetime.strptime(original_order['date'], "%Y-%m-%d %H:%M:%S%z")
        started = (original_order['status'] == "Completed")
        if started:
            start_price = float(original_order['created_price'])
            stop_loss_price = float(stop_loss_order['created_price'])
            take_profit_price = float(take_profit_order['created_price'])
            if stop_loss_order['status'] == 'Completed':
                end = datetime.strptime(stop_loss_order['date'], '%Y-%m-%d %H:%M:%S%z')
                # end = stop_loss_order['date']
                end_price = float(stop_loss_order['executed_price'])
                ended = True
            elif take_profit_order['status'] == 'Completed':
                end = datetime.strptime(take_profit_order['date'], '%Y-%m-%d %H:%M:%S%z')
                # end = take_profit_order['date']
                end_price = float(take_profit_order['executed_price'])
                ended = True
            else:
                end = end_of_dates
                end_price = end_of_prices
                ended = False
            if not end_price > 0:
                AssertionError("not end_price > 0")
            side = OrderSide.Buy if float(original_order['created_size']) > 0 else OrderSide.Sell
            if side == OrderSide.Buy:
                if not float(original_order['created_size']) > 0:
                    AssertionError("not original_order['created_size'] > 0")
            else:
                if not float(original_order['created_size']) < 0:
                    AssertionError("not original_order['created_size'] < 0")
            # if type(start) != str:
            #     name = start.strftime("%m-%d %H:%M")
            # else:
            #     name = start
            draw_base_pattern_orders(fig, legend_group, side, start, end, start_price, end_price, stop_loss_price,
                                     take_profit_price, float(original_order['created_size']),
                                     start.strftime("%m-%d %H:%M"), ended, order_group_id)
    return fig


"""
(orders_df: pd.DataFrame, fig: plotly.graph_objs.Figure,
end: datetime, end_of_prices: float,
base_pattern_date: pd.Timestamp, base_pattern_timeframe: str,
base_pattern: pt.Series[MultiTimeframeBasePattern])
"""


# def draw_band_order_groups(order_groups: Dict[int, tuple[bt.Order, bt.Order, bt.Order]], fig: plotly.graph_objs.Figure,
#                            end_of_dates: datetime, end_of_prices: float,
#                            base_pattern_date: pd.Timestamp, base_pattern_timeframe: str,
#                            legend_group: str):
#     order_group_ids = \
#         [key for key, (original_order, _, _) in order_groups.items()
#          if (
#                  (original_order.info['signal_index'][SignalDf.index_id('ref_date')] == base_pattern_date)
#                  and (original_order.info['signal_index'][SignalDf.index_id('ref_timeframe')] == base_pattern_timeframe)
#          )]
#
#     for order_group_id in order_groups.keys():
#         original_order: bt.Order
#         stop_loss_order: bt.Order
#         take_profit_order: bt.Order
#         original_order, stop_loss_order, take_profit_order = order_groups[order_group_id]
#         start = bt.num2date(original_order.created.dt).replace(tzinfo=pytz.UTC)
#         start_price = original_order.created.price
#         stop_loss_price = stop_loss_order.created.price
#         take_profit_price = take_profit_order.created.price
#         if stop_loss_order.status == bt.Order.Completed:
#             end_of_dates = bt.num2date(stop_loss_order.executed.dt).replace(tzinfo=pytz.UTC)
#             end_price = stop_loss_order.executed.price
#         elif take_profit_order.status == bt.Order.Completed:
#             end_of_dates = bt.num2date(take_profit_order.executed.dt).replace(tzinfo=pytz.UTC)
#             end_price = take_profit_order.executed.price
#         else:
#             end_of_dates = end_of_dates
#             end_price = end_of_prices
#         side = OrderSide.Buy if original_order.size > 0 else OrderSide.Sell
#         started = (original_order.status == bt.Order.Completed)
#         draw_base_pattern_orders(fig, legend_group, side, start, end_of_dates, start_price, end_price, stop_loss_price,
#                                  take_profit_price, original_order.created.size, start.strftime("%m-%d %H:%M"),
#                                  started, order_group_id)


def draw_base_pattern_orders(fig, legend_group: str, side: OrderSide, start: datetime, end: datetime,
                             start_price: float, end_price: float, stop_loss_price: float,
                             take_profit_price: float, size: float, name: str, ended: bool, order_group_id: int):
    pnl_value = (end_price - start_price) * size
    text = ",".join([
        # f"{side.value.upper()}@{start.strftime('%m/%d-%H:%M')}\r\n"
        f"{start_price:.0f}",
        f"SL{stop_loss_price:.0f}",
        f"TP{take_profit_price:.0f}",
        f"PNL{pnl_value:.1f}{'???' if not ended else ''}",  # /{pnl_percent}"
        # f"Base={legend_group}"
    ]
    )
    if abs(pnl_value) < 0.5:
        pass
    # draw stop-loss box
    xs = [start, end, end, start]
    ys = [start_price, start_price, stop_loss_price, stop_loss_price]
    fill = "toself" if ended else None
    line_width = 0 if ended else 0.5
    fig.add_scatter(x=xs, y=ys, fill=fill,  # fillcolor=fill_color,
                    fillpattern=dict(fgopacity=0.01), name=f"{name}={pnl_value:.0f}G{order_group_id}",
                    line=dict(color='red', width=line_width, dash='dash' if not ended else None),
                    mode='lines',
                    showlegend=False,
                    legendgroup=legend_group,
                    hovertemplate="%{text}", text=[text] * 4,
                    )
    # draw take_profit box
    xs = [start, end, end, start]
    ys = [start_price, start_price, take_profit_price, take_profit_price]
    fig.add_scatter(x=xs, y=ys, fill=fill,  # fillcolor=fill_color,
                    fillpattern=dict(fgopacity=0.01), name=f"{name}={pnl_value:.0f}G{order_group_id}",
                    line=dict(color='green', width=line_width, dash='dash' if not ended else None),
                    mode='lines',
                    showlegend=False,
                    legendgroup=legend_group,
                    hovertemplate="%{text}", text=[text] * 4,
                    )
    # draw pnl lien
    line_color = 'green' if pnl_value > 0 else 'red'
    xs = [start, end]
    ys = [start_price, end_price]
    fig.add_scatter(x=xs, y=ys,
                    name=f"{name}={pnl_value:.0f}G{order_group_id}",
                    line=dict(color=line_color, width=1, dash='dash' if not ended else None),
                    mode='lines',
                    showlegend=False,
                    hovertemplate="%{text}", text=[text] * 4,
                    legendgroup=legend_group,
                    )
    # add order pointing mark
    if side == OrderSide.Buy:
        _y = take_profit_price + 5
        symbol = "triangle-up"
    else:  # side == OrderSide.Sell
        _y = take_profit_price - 5
        symbol = "triangle-down"
    color = "green" if pnl_value > 0 else "red"
    fig.add_scatter(x=[start], y=[_y], mode="markers", name=f"{name}={pnl_value:.0f}G{order_group_id}",
                    marker=dict(symbol=symbol, color=color), text=[text],
                    hovertemplate="%{text}",
                    legendgroup=legend_group, )


def draw_base(base_pattern, fig, index_date, start, timeframe):
    xs = [start, base_pattern['effective_end'], base_pattern['effective_end'], start]
    ys = [base_pattern['internal_low'], base_pattern['internal_low'],
          base_pattern['internal_high'], base_pattern['internal_high']]
    fill_color = 'blue'
    legendgroup = MultiTimeframeBasePattern.str(index_date, timeframe, base_pattern)
    text = MultiTimeframeBasePattern.repr(index_date, timeframe, base_pattern)
    fig.add_scatter(x=xs, y=ys, fill="toself",  # fillcolor=fill_color,
                    fillpattern=dict(fgopacity=0.5),
                    name=legendgroup,
                    text=text,
                    line=dict(color=fill_color, width=0),
                    mode='lines',
                    legendgroup=legendgroup,
                    hoverinfo='text'
                    )
    return legendgroup, text
