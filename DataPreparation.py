import os
from datetime import datetime
from typing import Callable, Union

import numpy as np
import pandas as pd
import talib as ta
from pandas import Timedelta, DatetimeIndex, Timestamp
from plotly import graph_objects as plgo

from Config import config, CandleSize, GLOBAL_CACHE
from FigurePlotters import plot_multiple_figures, DEBUG
from helper import log


def generate_test_ohlc():
    test_ohlc_ticks = pd.read_csv(f'{config.files_to_load[0]}.zip', sep=',', header=0, index_col='date',
                                  parse_dates=['date'], skiprows=range(1, 400320), nrows=1440)
    file_name = f'ohlc.{file_id(test_ohlc_ticks)}.zip'
    test_ohlc_ticks.to_csv(file_name, compression='zip')


def insert_atr(single_timeframe_ohlc: pd.DataFrame) -> pd.DataFrame:
    _ATR = ta.ATR(high=single_timeframe_ohlc['high'].values, low=single_timeframe_ohlc['low'].values,
                  close=single_timeframe_ohlc['close'].values)
    single_timeframe_ohlc['ATR'] = _ATR
    return single_timeframe_ohlc


def range_of_data(data: pd.DataFrame) -> str:
    return f'{data.index.get_level_values("date")[0].strftime("%y-%m-%d.%H-%M")}T' \
           f'{data.index.get_level_values("date")[-1].strftime("%y-%m-%d.%H-%M")}'


def file_id(data: pd.DataFrame, name: str = '') -> str:
    if name is None or name == '':
        return f'{range_of_data(data)}'
    else:
        return f'{name}.{range_of_data(data)}'


def plot_ohlc(ohlc: pd = pd.DataFrame(columns=['open', 'high', 'low', 'close']),
              save: bool = False, name: str = '', show: bool = True) -> plgo.Figure:
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
    import os
    MAX_LEN_OF_DATA_FRAME_TO_PLOT = 50000
    SAFE_LEN_OF_DATA_FRAME_TO_PLOT = 10000
    if len(ohlc.index) > MAX_LEN_OF_DATA_FRAME_TO_PLOT:
        raise Exception(f'Too many rows to plt ({len(ohlc.index),}>{MAX_LEN_OF_DATA_FRAME_TO_PLOT})')
    if len(ohlc.index) > SAFE_LEN_OF_DATA_FRAME_TO_PLOT:
        print(f'Plotting too much data will slow us down ({len(ohlc.index),}>{SAFE_LEN_OF_DATA_FRAME_TO_PLOT})')
    if not os.path.isfile('kaleido.installed'):
        print('kaleido not satisfied!')
        try:
            os.system('pip install -q condacolab')
            import condacolab

            if not condacolab.check():
                condacolab.install()
                os.system('conda install -c conda-forge python-kaleido')
                os.system('echo "" > kaleido.installed')
            else:
                print('condacolab already satisfied')
        except:
            os.system('pip install -U kaleido')
            os.system('echo "" > kaleido.installed')
    if DEBUG: print(f'data({ohlc.shape})')
    if DEBUG: print(ohlc)
    fig = plgo.Figure(data=[plgo.Candlestick(x=ohlc.index.values,
                                             open=ohlc['open'], high=ohlc['high'], low=ohlc['low'],
                                             close=ohlc['close'],
                                             )], ).update_yaxes(fixedrange=False).update_layout(yaxis_title=name)
    if show: fig.show()
    if save:
        file_name = f'ohlc.{file_id(ohlc)}' if name == '' else f'ohlc.{name}.{file_id(ohlc)}'
        save_figure(fig, file_name)

    return fig


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
    fig = add_atr_boundary(fig, ohlca.index, midpoints=midpoints, widths=CandleSize.Spinning.value[1] * ohlca['ATR'],
                           name='Spinning')
    fig = add_atr_boundary(fig, ohlca.index, midpoints=midpoints, widths=CandleSize.Standard.value[1] * ohlca['ATR'],
                           name='Standard')
    fig = add_atr_boundary(fig, ohlca.index, midpoints=midpoints, widths=CandleSize.Long.value[1] * ohlca['ATR'],
                           name='Long')

    # Show the figure or write it to an HTML file
    if save:
        file_name = f'ohlca.{file_id(ohlca, name)}'
        save_figure(fig, file_name)

    if show:
        fig.show()
    return fig


def add_atr_boundary(fig: plgo.Figure, xs: pd.Series, midpoints: pd.Series, widths: pd.Series,
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
            line=dict(color='gray', dash='solid', width=0.2),
            fill='toself',
            fillcolor=f'rgba(128, 128, 128, {transparency})',  # 50% transparent gray color
            name=name
        )
    )


def validate_no_timeframe(data: pd.DataFrame) -> pd.DataFrame:
    if 'timeframe' in data.index.names:
        raise Exception(f'timeframe found in Data(indexes:{data.index.names}, columns:{data.columns.names}')
    return data


def save_figure(fig: plgo.Figure, file_name: str, file_path: str = '') -> None:
    """
    Save a Plotly figure as an HTML file.

    Parameters:
        fig (plotly.graph_objects.Figure): The Plotly figure to be saved.
        file_name (str): The name of the output HTML file (without extension).
        file_path (str, optional): The path to the directory where the HTML file will be saved.
                                  If not provided, the default path will be used.

    Returns:
        None

    Example:
        # Assuming you have a Plotly figure 'fig' and want to save it as 'my_plot.html'
        save_figure(fig, file_name='my_plot')

    Note:
        This function uses the Plotly 'write_html' method to save the figure as an HTML file.
    """
    if file_path == '':
        file_path = config.path_of_plots
    if not os.path.exists(file_path):
        os.mkdir(file_path)

    file_path = os.path.join(file_path, f'{file_name}.html')
    fig.write_html(file_path)


def generate_ohlca(date_range_str: str, file_path: str = config.path_of_data) -> None:
    # if not input_file_path.startswith('ohlc') or input_file_path.startswith('ohlca'):
    #     raise Exception('input_file expected to start with "ohlc" and does not start with "ohlca"!')
    ohlc = pd.read_csv(f'ohlc.{date_range_str}.zip', sep=',', header=0, index_col='date', parse_dates=['date'])
    ohlca = insert_atr(ohlc)
    plot_ohlca(ohlca)
    ohlca.to_csv(os.path.join(file_path, f'ohlca.{date_range_str}.zip'), compression='zip')


def plot_multi_timeframe_ohlca(multi_timeframe_ohlca, name: str = '', show: bool = True, save: bool = True):
    # todo: test plot_multi_timeframe_ohlca
    figures = []
    for _, timeframe in enumerate(config.timeframes):
        figures.append(plot_ohlca(single_timeframe(multi_timeframe_ohlca, timeframe), show=False, save=False,
                                  name=f'{timeframe} ohlca'))
    plot_multiple_figures(figures, name=f'multi_timeframe_ohlca.{file_id(multi_timeframe_ohlca, name)}',
                          save=save, show=show)


def generate_multi_timeframe_ohlca(date_range_str: str = config.under_process_date_range,
                                   file_path: str = config.path_of_data) -> None:
    multi_timeframe_ohlc = read_multi_timeframe_ohlc(date_range_str)
    multi_timeframe_ohlca = insert_atr(multi_timeframe_ohlc)
    plot_multi_timeframe_ohlca(multi_timeframe_ohlca)
    multi_timeframe_ohlca.to_csv(os.path.join(file_path, f'multi_timeframe_ohlca.{date_range_str}.zip'),
                                 compression='zip')


def plot_multi_timeframe_ohlc(multi_timeframe_ohlc, date_range_str):
    # todo: test plot_multi_timeframe_ohlc
    figures = []
    for _, timeframe in enumerate(config.timeframes):
        figures.append(plot_ohlc(single_timeframe(multi_timeframe_ohlc, timeframe)))
    plot_multiple_figures(figures, name=f'multi_timeframe_ohlc.{date_range_str}')


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
    plot_multi_timeframe_ohlc(ohlc, date_range_str)
    multi_timeframe_ohlc.to_csv(os.path.join(file_path, f'multi_timeframe_ohlc.{date_range_str}.zip'),
                                compression='zip')


def read_multi_timeframe_ohlc(date_range_str: str = config.under_process_date_range) -> pd.DataFrame:
    return read_file(date_range_str, 'multi_timeframe_ohlc', generate_multi_timeframe_ohlc)


def read_file(date_range_str: str, data_frame_type: str, generator: Callable, skip_rows=None,
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


def timedelta_to_str(input_time: Union[str, Timedelta]) -> str:
    if isinstance(input_time, str):
        timedelta_obj = Timedelta(input_time)
    elif isinstance(input_time, Timedelta):
        timedelta_obj = input_time
    else:
        raise ValueError("Input should be either a pandas timedelta string or a pandas Timedelta object.")

    total_minutes = timedelta_obj.total_seconds() // 60
    hours = int(total_minutes // 60)
    minutes = int(total_minutes % 60)

    if hours == 0: hours = ''

    return f"{hours}:{minutes}"


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
    if 'timeframe' not in multi_timeframe_data.index.names:
        raise Exception(
            f'multi_timeframe_data expected to have "timeframe" in indexes:[{multi_timeframe_data.index.names}]')
    single_timeframe_data: pd.DataFrame = multi_timeframe_data.loc[
        multi_timeframe_data.index.get_level_values('timeframe') == timeframe]
    return validate_no_timeframe(single_timeframe_data.droplevel('timeframe'))


def to_timeframe(time: Union[DatetimeIndex, datetime], timeframe: str) -> datetime:
    """
    Round the given datetime to the nearest time based on the specified timeframe.

    Parameters:
        time (datetime): The datetime to be rounded.
        timeframe (str): The desired timeframe (e.g., '1min', '5min', '1H', etc.).

    Returns:
        datetime: The rounded datetime that corresponds to the nearest time within the specified timeframe.
    """
    # Calculate the timedelta for the specified timeframe
    timeframe_timedelta = pd.to_timedelta(timeframe)

    # Calculate the number of seconds in the timedelta
    seconds_in_timeframe = timeframe_timedelta.total_seconds()
    if isinstance(time, DatetimeIndex):
        # Calculate the timestamp with the floor division
        rounded_timestamp = ((time.view(np.int64) // 10 ** 9) // seconds_in_timeframe) * seconds_in_timeframe

        # Convert the rounded timestamp back to datetime
        rounded_time = pd.DatetimeIndex(rounded_timestamp * 10 ** 9)
        for t in rounded_time:
            if t not in GLOBAL_CACHE[f'ohlc_{timeframe}']:
                raise Exception(f'Invalid time {t}!')
    # elif isinstance(time, datetime):
    #     # Calculate the timestamp with the floor division
    #     rounded_timestamp = (time.timestamp() // seconds_in_timeframe) * seconds_in_timeframe
    #
    #     # Convert the rounded timestamp back to datetime
    #     rounded_time = datetime.fromtimestamp(rounded_timestamp)
    elif isinstance(time, Timestamp):
        rounded_timestamp = (time.timestamp() // seconds_in_timeframe) * seconds_in_timeframe

        # Convert the rounded timestamp back to datetime
        rounded_time = pd.Timestamp(rounded_timestamp * 10 ** 9)
        if rounded_time not in GLOBAL_CACHE[f'ohlc_{timeframe}']:
            raise Exception(f'Invalid time {rounded_time}!')
    else:
        raise Exception(f'Invalid type of time:{type(time)}')
    return rounded_time


def test_index_match_timeframe(data: pd.DataFrame, timeframe: str):
    for index_value, mapped_index_value in map(lambda x, y: (x, y), data.index, to_timeframe(data.index, timeframe)):
        if index_value != mapped_index_value:
            raise Exception(
                f'In Data({data.columns.names}) found Index({index_value}) not align with timeframe:{timeframe}/{mapped_index_value}\n'
                f'Indexes:{data.index.values}')
