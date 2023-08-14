import os
from datetime import datetime
from typing import Callable, Union

import numpy as np
import pandas as pd
from pandas import Timedelta, DatetimeIndex, Timestamp

from Config import config, GLOBAL_CACHE


def range_of_data(data: pd.DataFrame) -> str:
    return f'{data.index.get_level_values("date")[0].strftime("%y-%m-%d.%H-%M")}T' \
           f'{data.index.get_level_values("date")[-1].strftime("%y-%m-%d.%H-%M")}'


def read_file(date_range_str: str, data_frame_type: str, generator: Callable, skip_rows=None,
              n_rows=None, file_path: str = config.path_of_data) -> pd.DataFrame:
    # todo: add cache to read_file
    df = None
    try:
        df = read_with_timeframe(data_frame_type, date_range_str, file_path, n_rows, skip_rows)
    except Exception as e:
        pass
    if (data_frame_type + '_columns') not in config.__dir__():
        raise Exception(data_frame_type + '_columns not defined in configuration!')
    if df is None or not check_dataframe(df, getattr(config, data_frame_type + '_columns')):
        generator(date_range_str)
        df = read_with_timeframe(data_frame_type, date_range_str, file_path, n_rows, skip_rows)
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


def read_with_timeframe(data_frame_type, date_range_str, file_path, n_rows, skip_rows):
    df = pd.read_csv(os.path.join(file_path, f'{data_frame_type}.{date_range_str}.zip'), sep=',', header=0,
                     index_col='date', parse_dates=['date'], skiprows=skip_rows, nrows=n_rows)
    if 'multi_timeframe' in data_frame_type:
        df.set_index('timeframe', append=True, inplace=True)
        df = df.swaplevel()
    return df


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


def single_timeframe(multi_timeframe_data: pd.DataFrame, timeframe):
    if 'timeframe' not in multi_timeframe_data.index.names:
        raise Exception(
            f'multi_timeframe_data expected to have "timeframe" in indexes:[{multi_timeframe_data.index.names}]')
    if timeframe not in config.timeframes:
        raise Exception(
            f'timeframe:{timeframe} is not in supported timeframes:{config.timeframes}')
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
            if t not in GLOBAL_CACHE[f'valid_times_{timeframe}']:
                raise Exception(f'Invalid time {t}!')
    elif isinstance(time, Timestamp):
        rounded_timestamp = (time.timestamp() // seconds_in_timeframe) * seconds_in_timeframe

        # Convert the rounded timestamp back to datetime
        rounded_time = pd.Timestamp(rounded_timestamp * 10 ** 9)
        if f'valid_times_{timeframe}' not in GLOBAL_CACHE.keys():
            raise Exception(f'valid_times_{timeframe} not initialized in GLOBAL_CACHE')
        if rounded_time not in GLOBAL_CACHE[f'valid_times_{timeframe}']:
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


def validate_no_timeframe(data: pd.DataFrame) -> pd.DataFrame:
    if 'timeframe' in data.index.names:
        raise Exception(f'timeframe found in Data(indexes:{data.index.names}, columns:{data.columns.names}')
    return data
