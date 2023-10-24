import pandas as pd
from plotly import graph_objects as plgo

from Config import config, CandleSize
from DataPreparation import single_timeframe
from FigurePlotter.plotter import plot_multiple_figures, file_id, DEBUG, save_figure
from helper import log, measure_time


@measure_time
def plot_multi_timeframe_ohlcva(multi_timeframe_ohlcva, name: str = '', show: bool = True, save: bool = True) -> None:
    # todo: test plot_multi_timeframe_ohlcva
    figures = []
    for _, timeframe in enumerate(config.timeframes):
        figures.append(plot_ohlcva(single_timeframe(multi_timeframe_ohlcva, timeframe), show=False, save=False,
                                  name=f'{timeframe} ohlcva'))
    plot_multiple_figures(figures, name=f'multi_timeframe_ohlcva.{file_id(multi_timeframe_ohlcva, name)}',
                          save=save, show=show)


# @measure_time
def plot_multi_timeframe_ohlcv(multi_timeframe_ohlcv, date_range_str):
    # todo: test plot_multi_timeframe_ohlcv
    figures = []
    for _, timeframe in enumerate(config.timeframes):
        figures.append(plot_ohlcv(single_timeframe(multi_timeframe_ohlcv, timeframe), show=False, save=False,
                                  name=f'{timeframe} ohlcv'))
    plot_multiple_figures(figures, name=f'multi_timeframe_ohlcv.{date_range_str}')


# @measure_time
def plot_ohlcv(ohlcv: pd = pd.DataFrame(columns=['open', 'high', 'low', 'close']),
               save: bool = True, name: str = '', show: bool = True) -> plgo.Figure:
    """
        Plot OHLC (Open, High, Low, Close) data as a candlestick chart.

        Parameters:
            ohlcv (pd.DataFrame): A DataFrame containing OHLC data.
            save (bool): If True, the plot is saved as an image file.
            name (str): The name of the plot.
            show (bool): If False, the plot will not be displayed.

        Returns:
            plgo.Figure: The Plotly figure object containing the OHLC candlestick chart.
        """
    # MAX_LEN_OF_DATA_FRAME_TO_PLOT = 50000
    # SAFE_LEN_OF_DATA_FRAME_TO_PLOT = 10000
    # if len(ohlcv.index) > MAX_LEN_OF_DATA_FRAME_TO_PLOT:
    #     raise Exception(f'Too many rows to plt ({len(ohlcv.index),}>{MAX_LEN_OF_DATA_FRAME_TO_PLOT})')
    # if len(ohlcv.index) > SAFE_LEN_OF_DATA_FRAME_TO_PLOT:
    #     log(f'Plotting too much data will slow us down ({len(ohlcv.index),}>{SAFE_LEN_OF_DATA_FRAME_TO_PLOT})')

    kaleido_install_lock_file_path = 'kaleido.installed'
    # if not os.path.isfile(kaleido_install_lock_file_path):
    #     log('kaleido not satisfied!')
    #     try:
    #         os.system('pip install -q condacolab')
    #         import condacolab
    #
    #         if not condacolab.check():
    #             condacolab.install()
    #             os.system('conda install -c conda-forge python-kaleido')
    #             os.system(f'echo "" > {kaleido_install_lock_file_path}')
    #         else:
    #             log('condacolab already satisfied')
    #     except:
    #         os.system('pip install -U kaleido')
    #         os.system(f'echo "" > {kaleido_install_lock_file_path}')
    if DEBUG: log(f'data({ohlcv.shape})')
    if DEBUG: log(ohlcv)
    fig = plgo.Figure(data=[plgo.Candlestick(x=ohlcv.index.values,
                                             open=ohlcv['open'], high=ohlcv['high'], low=ohlcv['low'], close=ohlcv['close']
                                             , name=name
                                             )]).update_yaxes(fixedrange=False).update_layout(yaxis_title=name)
    if show: fig.show()
    if save:
        file_name = f'ohlcv.{file_id(ohlcv, name)}'
        save_figure(fig, file_name)

    return fig


# @measure_time
def plot_ohlcva(ohlcva: pd.DataFrame, save: bool = True, show: bool = True, name: str = '') -> plgo.Figure:
    """
    Plot OHLC data with an additional ATR (Average True Range) boundary.

    The function plots OHLC data as a candlestick chart and adds an ATR boundary to the plot.
    The boundary's middle is calculated as the average of the candle's open and close,
    and the width of the boundary is equal to the ATR value for each data point.

    Parameters:
        ohlcva (pd.DataFrame): A DataFrame containing OHLC data along with the 'ATR' column representing the ATR values.
        save (bool): If True, the plot is saved as an HTML file.
        show (bool): If True, the plot is displayed in the browser.

    Returns:
        None

    Example:
        # Assuming you have the 'ohlcva' DataFrame with the required columns (open, high, low, close, ATR)
        date_range_str = "17-10-06.00-00T17-10-06"
        plot_ohlcva(ohlcva, date_range_str)
    """
    # Calculate the middle of the boundary (average of open and close)
    midpoints = (ohlcva['high'] + ohlcva['low']) / 2
    # Create a figure using the plot_ohlcv function
    fig = plot_ohlcv(ohlcva[['open', 'high', 'low', 'close']], show=False, save=False, name=name)

    # Add the ATR boundaries
    fig = add_atr_scatter(fig, ohlcva.index, midpoints=midpoints, widths=CandleSize.Spinning.value[1] * ohlcva['ATR'],
                          name='Standard')
    fig = add_atr_scatter(fig, ohlcva.index, midpoints=midpoints, widths=CandleSize.Standard.value[1] * ohlcva['ATR'],
                          name='Long')
    fig = add_atr_scatter(fig, ohlcva.index, midpoints=midpoints, widths=CandleSize.Long.value[1] * ohlcva['ATR'],
                          name='Spike')

    fig.add_scatter(x=ohlcva.index, y=midpoints,
                    mode='none',
                    showlegend=False,
                    text=[f'ATR: {atr:0.1f}' for atr in ohlcva['ATR']],
                    hoverinfo='text')

    fig.update_layout(hovermode='x unified')
    # fig.add_scatter(x=ohlcva.index, y=(ohlcva['high']+ohlcva['low'])/2, mode='text', text=f'ATR: {ohlcva["ATR"]}')
    # Show the figure or write it to an HTML file
    if save:
        file_name = f'ohlcva.{file_id(ohlcva, name)}'
        save_figure(fig, file_name)

    if show:
        fig.show()
    return fig


def add_atr_scatter(fig: plgo.Figure, xs: pd.Series, midpoints: pd.Series, widths: pd.Series,
                    transparency: float = 0.2, name: str = 'ATR') -> plgo.Figure:
    xs = xs.tolist()
    half_widths = widths.fillna(value=0).div(2)
    upper_band: pd.Series = midpoints + half_widths
    lower_band: pd.Series = midpoints - half_widths

    return fig.add_trace(
        plgo.Scatter(
            x=xs + xs[::-1],
            y=upper_band.tolist() + lower_band.tolist()[::-1],
            mode='lines',
            # hovertext=widths,
            line=dict(color='gray', dash='solid', width=0.2),
            fill='toself',
            fillcolor=f'rgba(128, 128, 128, {transparency})',  # 50% transparent gray color
            name=name
        )
    )
