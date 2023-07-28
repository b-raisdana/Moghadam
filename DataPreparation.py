import os
import typing
import pandas as pd
import talib as ta
from plotly import graph_objects as plgo

from Config import config
from FigurePlotters import plot_ohlc, plot_multiple_figures
from helper import log


def generate_test_ohlc():
    test_ohlc_ticks = pd.read_csv(f'{config.files_to_load[0]}.zip', sep=',', header=0, index_col='date',
                                  parse_dates=['date'], skiprows=range(1, 400320), nrows=1440)
    file_name = f'ohlc.{test_ohlc_ticks.index[0].strftime("%y-%m-%d.%H-%M")}T' \
                f'{test_ohlc_ticks.index[-1].strftime("%y-%m-%d.%H-%M")}.zip'
    test_ohlc_ticks.to_csv(file_name, compression='zip')


def insert_atr(single_timeframe_ohlc: pd.DataFrame) -> pd.DataFrame:
    _ATR = ta.ATR(high=single_timeframe_ohlc['high'].values, low=single_timeframe_ohlc['low'].values,
                  close=single_timeframe_ohlc['close'].values)
    single_timeframe_ohlc['ATR'] = _ATR
    return single_timeframe_ohlc


def plot_ohlca(ohlca: pd.DataFrame, date_range_str: str, save: bool = True, show: bool = True):
    """
    Plot OHLC data with an additional ATR (Average True Range) boundary.

    The function plots OHLC data as a candlestick chart and adds an ATR boundary to the plot.
    The boundary's middle is calculated as the average of the candle's open and close,
    and the width of the boundary is equal to the ATR value for each data point.

    Parameters:
        ohlca (pd.DataFrame): A DataFrame containing OHLC data along with the 'ATR' column representing the ATR values.
        date_range_str (str): The date range string to identify the plot in the format 'YY-MM-DD.HH-MMTYY-MM-DD.HH-MM'.
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
    ohlca['boundary_mid'] = (ohlca['open'] + ohlca['close']) / 2

    # Create a figure using the plot_ohlc function
    fig = plot_ohlc(ohlca[['open', 'high', 'low', 'close']], save=False, name='')

    # Add scatter trace for the ATR boundary
    fig.add_trace(
        plgo.Scatter(
            x=ohlca.index,
            y=ohlca['boundary_mid'],
            mode='lines',
            line=dict(color='cray', width=ohlca['ATR'], dash='solid'),
            fill='tozeroy',
            fillcolor='rgba(128, 128, 128, 0.5)',  # 50% transparent cray color
            name='ATR Boundary'
        )
    )

    # Show the figure or write it to an HTML file
    if save:
        if not os.path.exists(config.path_of_plots):
            os.mkdir(config.path_of_plots)
        file_name = f'ohlca.{date_range_str}.html'
        file_path = os.path.join(config.path_of_plots, file_name)
        fig.write_html(file_path)

    if show:
        fig.show()


def generate_ohlca(date_range_str: str, file_path: str = config.path_of_data) -> None:
    # if not input_file_path.startswith('ohlc') or input_file_path.startswith('ohlca'):
    #     raise Exception('input_file expected to start with "ohlc" and does not start with "ohlca"!')
    ohlc = pd.read_csv(f'ohlc.{date_range_str}.zip', sep=',', header=0, index_col='date', parse_dates=['date'])
    ohlca = insert_atr(ohlc)
    ohlca.to_csv(os.path.join(file_path, f'ohlca.{date_range_str}.zip'), compression='zip')
    plot_ohlca(ohlca)


def plot_multi_timeframe_ohlca(multi_timeframe_ohlca, date_range_str: str):
    # todo: test plot_multi_timeframe_ohlca
    figures = []
    for _, timeframe in enumerate(config.timeframes):
        figures.append(plot_ohlca(single_timeframe(multi_timeframe_ohlca, timeframe)))
    plot_multiple_figures(figures, file_name=f'multi_timeframe_ohlc.{date_range_str}.html')


def generate_multi_timeframe_ohlca(date_range_str: str = config.under_process_date_range,
                                   file_path: str = config.path_of_data) -> None:
    multi_timeframe_ohlc = read_multi_timeframe_ohlc(date_range_str)
    multi_timeframe_ohlca = insert_atr(multi_timeframe_ohlc)
    multi_timeframe_ohlca.to_csv(os.path.join(file_path, f'multi_timeframe_ohlca.{date_range_str}.zip'),
                                 compression='zip')
    plot_multi_timeframe_ohlca(multi_timeframe_ohlca)


def plot_multi_timeframe_ohlc(multi_timeframe_ohlc, date_range_str):
    # todo: test plot_multi_timeframe_ohlc
    figures = []
    for _, timeframe in enumerate(config.timeframes):
        figures.append(plot_ohlc(single_timeframe(multi_timeframe_ohlc, timeframe)))
    plot_multiple_figures(figures, file_name=f'multi_timeframe_ohlc.{date_range_str}.html')


def generate_multi_timeframe_ohlc(date_range_str: str, file_path: str = config.path_of_data):
    ohlc = read_ohlc(date_range_str)
    # ohlc['timeframe '] = config.timeframes[0]
    multi_timeframe_ohlc = ohlc.copy()
    multi_timeframe_ohlc.insert(0, 'timeframe', config.timeframes[0])
    multi_timeframe_ohlc.set_index('timeframe', append=True, inplace=True)
    multi_timeframe_ohlc = multi_timeframe_ohlc.swaplevel()
    for _, timeframe in enumerate(config.timeframes[1:]):
        _timeframe_ohlc = ohlc.groupby(pd.Grouper(freq=timeframe)) \
            .agg({'open': 'first',
                  'close': 'last',
                  'low': 'min',
                  'high': 'max',
                  'volume': 'sum', })
        _timeframe_ohlc.insert(0, 'timeframe', timeframe)
        _timeframe_ohlc.set_index('timeframe', append=True, inplace=True)
        _timeframe_ohlc = _timeframe_ohlc.swaplevel()
        multi_timeframe_ohlc = pd.concat([multi_timeframe_ohlc, _timeframe_ohlc])
    multi_timeframe_ohlc = multi_timeframe_ohlc.sort_index()
    multi_timeframe_ohlc.to_csv(os.path.join(file_path, f'multi_timeframe_ohlc.{date_range_str}.zip'),
                                compression='zip')
    plot_multi_timeframe_ohlc(ohlc, date_range_str)


def read_multi_timeframe_ohlc(date_range_str: str = config.under_process_date_range) -> pd.DataFrame:
    return read_file(date_range_str, 'multi_timeframe_ohlc', generate_multi_timeframe_ohlc)


def read_file(date_range_str: str, data_frame_type: str, generator: typing.Callable, skip_rows=None,
              n_rows=None, file_path: str = config.path_of_data) -> pd.DataFrame:
    # todo: add cache to read_file
    df = None
    try:
        df = read_with_timeframe(data_frame_type, date_range_str, df, file_path, n_rows, skip_rows)
    except Exception as e:
        pass
    if (data_frame_type + '_columns') not in config.__dir__():
        raise Exception(data_frame_type + '_columns not defined in configuration!')
    if df is None or not check_dataframe(df, getattr(config, data_frame_type + '_columns')):
        generator(date_range_str)
        df = read_with_timeframe(data_frame_type, date_range_str, df, file_path, n_rows, skip_rows)
        if not check_dataframe(df, getattr(config, data_frame_type + '_columns')):
            raise Exception(f'Failed to generate {data_frame_type}! {data_frame_type}.columns:{df.columns}')
    return df


def read_with_timeframe(data_frame_type, date_range_str, df, file_path, n_rows, skip_rows):
    df = pd.read_csv(os.path.join(file_path, f'{data_frame_type}.{date_range_str}.zip'), sep=',', header=0,
                     index_col='date', parse_dates=['date'], skiprows=skip_rows, nrows=n_rows)
    if 'multi_timeframe' in data_frame_type:
        df.set_index('timeframe', append=True, inplace=True)
        df = df.swaplevel()
    return df


def check_multi_timeframe_ohlca_columns(multi_timeframe_ohlca: pd.DataFrame, raise_exception=False) -> bool:
    return check_dataframe(multi_timeframe_ohlca, config.multi_timeframe_ohlca_columns, raise_exception)


def check_dataframe(dataframe: pd.DataFrame, columns: [str], raise_exception=False):
    try:
        dataframe.columns
    except NameError:
        if raise_exception:
            raise Exception(
                f'The DataFrame does not have columns:{dataframe}')
        else:
            return False
    for _column in columns:
        if _column not in list(dataframe.columns) + list(dataframe.index.names):
            if raise_exception:
                raise Exception(
                    f'The DataFrame expected to contain {_column} but have these columns:{dataframe.columns}')
            else:
                return False
    return True


def read_multi_timeframe_ohlca(date_range_str: str = config.under_process_date_range) -> pd.DataFrame:
    return read_file(date_range_str, 'multi_timeframe_ohlca', generate_multi_timeframe_ohlca)


def read_ohlca(date_range_string: str) -> pd.DataFrame:
    return read_file(date_range_string, 'ohlca', generate_ohlca)


def read_ohlc(date_range_string: str) -> pd.DataFrame:
    try:
        return read_file(date_range_string, 'ohlc', generate_ohlc)
    except:
        log(f'Failed to load ohlc.{date_range_string} try to load ohlca.{date_range_string}')
        return read_file(date_range_string, 'ohlca', generate_ohlc)


def generate_ohlc(date_range_string: str):
    raise Exception('Not implemented so we expect to file exists.')


def single_timeframe(multi_timeframe_data: pd.DataFrame, timeframe):
    if multi_timeframe_data.index.names[0] != 'timeframe':
        raise Exception(
            f'Level 0 of a multi_timeframe_data expected to be "timeframe" but indexes are [{multi_timeframe_data.index.names}]')
    return multi_timeframe_data.loc[timeframe, :]
