import pandas as pd
from plotly import graph_objects as plgo

from Config import config, CandleSize
from DataPreparation import single_timeframe
from FigurePlotter.plotter import plot_multiple_figures, file_id, DEBUG, save_figure
from helper import log, measure_time


@measure_time
def plot_multi_timeframe_ohlca(multi_timeframe_ohlca, name: str = '', show: bool = True, save: bool = True) -> None:
    # todo: test plot_multi_timeframe_ohlca
    figures = []
    for _, timeframe in enumerate(config.timeframes):
        figures.append(plot_ohlca(single_timeframe(multi_timeframe_ohlca, timeframe), show=False, save=False,
                                  name=f'{timeframe} ohlca'))
    plot_multiple_figures(figures, name=f'multi_timeframe_ohlca.{file_id(multi_timeframe_ohlca, name)}',
                          save=save, show=show)


# @measure_time
def plot_multi_timeframe_ohlc(multi_timeframe_ohlc, date_range_str):
    # todo: test plot_multi_timeframe_ohlc
    figures = []
    for _, timeframe in enumerate(config.timeframes):
        figures.append(plot_ohlc(single_timeframe(multi_timeframe_ohlc, timeframe), show=False, save=False,
                                 name=f'{timeframe} ohlc'))
    plot_multiple_figures(figures, name=f'multi_timeframe_ohlc.{date_range_str}')


@measure_time
def plot_ohlc(ohlc: pd = pd.DataFrame(columns=['open', 'high', 'low', 'close']),
              save: bool = True, name: str = '', show: bool = True) -> plgo.Figure:
    """
        Plot OHLC (Open, High, Low, Close) data as a candlestick chart.

        Parameters:
            ohlc (pd.DataFrame): A DataFrame containing OHLC data.
            save (bool): If True, the plot is saved as an image file.
            name (str): The name of the plot.
            show (bool): If False, the plot will not be displayed.

        Returns:
            plgo.Figure: The Plotly figure object containing the OHLC candlestick chart.
        """
    # MAX_LEN_OF_DATA_FRAME_TO_PLOT = 50000
    # SAFE_LEN_OF_DATA_FRAME_TO_PLOT = 10000
    # if len(ohlc.index) > MAX_LEN_OF_DATA_FRAME_TO_PLOT:
    #     raise Exception(f'Too many rows to plt ({len(ohlc.index),}>{MAX_LEN_OF_DATA_FRAME_TO_PLOT})')
    # if len(ohlc.index) > SAFE_LEN_OF_DATA_FRAME_TO_PLOT:
    #     log(f'Plotting too much data will slow us down ({len(ohlc.index),}>{SAFE_LEN_OF_DATA_FRAME_TO_PLOT})')

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
    if DEBUG: log(f'data({ohlc.shape})')
    if DEBUG: log(ohlc)
    fig = plgo.Figure(data=[plgo.Candlestick(x=ohlc.index.values,
                                             open=ohlc['open'], high=ohlc['high'], low=ohlc['low'], close=ohlc['close']
                                             , name=name
                                             )]).update_yaxes(fixedrange=False).update_layout(yaxis_title=name)
    if show: fig.show()
    if save:
        file_name = f'ohlc.{file_id(ohlc, name)}'
        save_figure(fig, file_name)

    return fig


# @measure_time
def plot_ohlca(ohlca: pd.DataFrame, save: bool = True, show: bool = True, name: str = '') -> plgo.Figure:
    """
    Plot OHLC data with an additional ATR (Average True Range) boundary.

    The function plots OHLC data as a candlestick chart and adds an ATR boundary to the plot.
    The boundary's middle is calculated as the average of the candle's open and close,
    and the width of the boundary is equal to the ATR value for each data point.

    Parameters:
        ohlca (pd.DataFrame): A DataFrame containing OHLC data along with the 'ATR' column representing the ATR values.
        save (bool): If True, the plot is saved as an HTML file.
        show (bool): If True, the plot is displayed in the browser.

    Returns:
        None

    Example:
        # Assuming you have the 'ohlca' DataFrame with the required columns (open, high, low, close, ATR)
        date_range_str = "17-10-06.00-00T17-10-06"
        plot_ohlca(ohlca, date_range_str)
    """
    # Calculate the middle of the boundary (average of open and close)
    midpoints = (ohlca['high'] + ohlca['low']) / 2
    # Create a figure using the plot_ohlc function
    fig = plot_ohlc(ohlca[['open', 'high', 'low', 'close']], show=False, save=False, name=name)

    # Add the ATR boundaries
    fig = add_atr_scatter(fig, ohlca.index, midpoints=midpoints, widths=CandleSize.Spinning.value[1] * ohlca['ATR'],
                          name='Standard')
    fig = add_atr_scatter(fig, ohlca.index, midpoints=midpoints, widths=CandleSize.Standard.value[1] * ohlca['ATR'],
                          name='Long')
    fig = add_atr_scatter(fig, ohlca.index, midpoints=midpoints, widths=CandleSize.Long.value[1] * ohlca['ATR'],
                          name='Spike')

    fig.add_scatter(x=ohlca.index, y=midpoints,
                    mode='none',
                    showlegend=False,
                    text=[f'ATR: {atr:0.1f}' for atr in ohlca['ATR']],
                    hoverinfo='text')

    fig.update_layout(hovermode='x unified')
    # fig.add_scatter(x=ohlca.index, y=(ohlca['high']+ohlca['low'])/2, mode='text', text=f'ATR: {ohlca["ATR"]}')
    # Show the figure or write it to an HTML file
    if save:
        file_name = f'ohlca.{file_id(ohlca, name)}'
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
