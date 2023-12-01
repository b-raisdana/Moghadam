import os
import re
import string
from dataclasses import dataclass
from datetime import datetime, timedelta
from typing import Callable, Union, List, Type, TypeVar

import numpy as np
import pandas as pd
import pandera
import pytz
from pandas import Timedelta, DatetimeIndex, Timestamp
from pandera import typing as pt

from Config import config, GLOBAL_CACHE
from Model import MultiTimeframe
from helper import log, date_range, date_range_to_string, morning, measure_time


def range_of_data(data: pd.DataFrame) -> str:
    """
    Generate a formatted date range string based on the first and last timestamps in the DataFrame's index.

    This function calculates and returns a formatted string representing the date range of the provided DataFrame.
    The string format is 'yy-mm-dd.HH-MMTyy-mm-dd.HH-MM', where the first timestamp corresponds to the start of the
    date range and the last timestamp corresponds to the end of the date range.

    Parameters:
        data (pd.DataFrame): The DataFrame for which to generate the date range string.

    Returns:
        str: The formatted date range string.

    Example:
        # Assuming you have a DataFrame 'data' with an index containing timestamps
        date_range = range_of_data(data)
        print(date_range)  # Output: 'yy-mm-dd.HH-MMTyy-mm-dd.HH-MM'
    """
    return f'{data.index.get_level_values("date")[0].strftime("%y-%m-%d.%H-%M")}T' \
           f'{data.index.get_level_values("date")[-1].strftime("%y-%m-%d.%H-%M")}'


# Define a type variable
Pandera_DFM_Type = TypeVar('T', bound=pandera.DataFrameModel)


# @cache
def data_is_not_cachable(date_range_str):
    _, end = date_range(date_range_str)
    if end > morning(datetime.utcnow()):
        return True
    return False


def read_file(date_range_str: str, data_frame_type: str, generator: Callable, caster_model: Type[Pandera_DFM_Type]
              , skip_rows=None, n_rows=None, file_path: str = config.path_of_data,
              zero_size_allowed: Union[None, bool] = None) -> Pandera_DFM_Type:
    """
    Read data from a file and return a DataFrame. If the file does not exist or the DataFrame does not
    match the expected columns, the generator function is used to create the DataFrame.

    This function reads data from a file with the specified `data_frame_type` and `date_range_str` parameters.
    If the file exists and the DataFrame columns match the expected columns defined in the configuration, the
    function returns the DataFrame. Otherwise, the `generator` function is invoked to generate the DataFrame,
    and then the columns of the generated DataFrame are checked against the expected columns.

    Parameters:
        date_range_str (str): The date range string used to construct the filename.
        data_frame_type (str): The type of DataFrame to read, e.g., 'ohlcva', 'multi_timeframe_ohlcva', etc.
        generator (Callable): The function that generates the DataFrame if needed.
        skip_rows (Optional[int]): The number of rows to skip while reading the file.
        n_rows (Optional[int]): The maximum number of rows to read from the file.
        file_path (str, optional): The path to the directory containing the data files.

    Returns:
        pd.DataFrame: The DataFrame read from the file or generated by the generator function.

    Raises:
        Exception: If the expected columns are not defined in the configuration or if the generated DataFrame
                   does not match the expected columns.

    Example:
        # Assuming you have a generator function 'generate_ohlcva' and 'ohlcva_columns' defined in configuration
        df = read_file(date_range_str='17-10-06.00-00T17-10-06', data_frame_type='ohlcva',
                       generator=generate_ohlcva)

    Note:
        This function first attempts to read the file based on the provided parameters. If the file is not found
        or the DataFrame does not match the expected columns, the generator function is called to create the DataFrame.
        :param zero_size_allowed:
        :param file_path:
        :param n_rows:
        :param skip_rows:
        :param date_range_str:
        :param data_frame_type:
        :param generator:
        :param caster_model:
    """
    if date_range_str is None:
        date_range_str = config.under_process_date_range
    df = None
    try:
        df = read_with_timeframe(data_frame_type, date_range_str, file_path, n_rows, skip_rows)
    except FileNotFoundError as e:
        pass
    except Exception as e:
        raise e
    # if (data_frame_type + '_columns') not in config.__dir__():
    #     raise Exception(data_frame_type + '_columns not defined in configuration!')
    # if df is None or not check_dataframe(df, getattr(config, data_frame_type + '_columns')):
    #     generator(date_range_str)
    #     df = read_with_timeframe(data_frame_type, date_range_str, file_path, n_rows, skip_rows)
    #     if not check_dataframe(df, getattr(config, data_frame_type + '_columns')):
    #         raise Exception(f'Failed to generate {data_frame_type}! {data_frame_type}.columns:{df.columns}')
    #     # log(f'generate {data_frame_type} executed in {timedelta_to_str(datetime.now() - start_time, milliseconds=True)}s')

    if zero_size_allowed is None:
        zero_size_allowed = after_under_process_date(date_range_str)
    if df is None or not cast_and_validate(df, caster_model, return_bool=True, zero_size_allowed=zero_size_allowed):
        generator(date_range_str)
        df = read_with_timeframe(data_frame_type, date_range_str, file_path, n_rows, skip_rows)
        df = cast_and_validate(df, caster_model, zero_size_allowed=zero_size_allowed)
    else:
        df = cast_and_validate(df, caster_model, zero_size_allowed=zero_size_allowed)
    if data_is_not_cachable(date_range_str):
        os.remove(os.path.join(file_path, f'{data_frame_type}.{date_range_str}.zip'))
    return df


def df_timedelta_to_str(input_time: Union[str, Timedelta], hours=True, ignore_zero: bool = True) -> str:
    """
    Convert a pandas timedelta string or a pandas Timedelta object into a human-readable string representation.

    This function takes a pandas timedelta string or a pandas Timedelta object and converts it into a string format
    of hours and minutes. If the input is a string, it is converted to a Timedelta object. The resulting string
    represents the number of hours and minutes in the input timedelta.

    Parameters:
        input_time (Union[str, Timedelta]): The input timedelta, which can be a pandas timedelta string or a
                                           pandas Timedelta object.
        hours (bool, optional): If True (default), includes hours in the output. If False, only includes minutes.
        ignore_zero (bool, optional): If True (default), removes zero values from the output.

    Returns:
        str: A string representation of the input timedelta in the format "hours:minutes".

    Raises:
        ValueError: If the input is not a pandas timedelta string or a pandas Timedelta object.

    Example:
        # Convert a timedelta string to a human-readable string
        time_str = "2 days 03:30:00"
        result = df_timedelta_to_str(time_str)  # Result: "51:30"

        # Convert a Timedelta object to a human-readable string
        import pandas as pd
        time_delta = pd.Timedelta(days=2, hours=3, minutes=30)
        result = df_timedelta_to_str(time_delta)  # Result: "51:30"
    """
    if isinstance(input_time, str):
        timedelta_obj = Timedelta(input_time)
    elif (isinstance(input_time, Timedelta)
          or isinstance(input_time, np.timedelta64)
          or isinstance(input_time, pt.Timedelta)):
        timedelta_obj = input_time
    else:
        raise ValueError("Input should be either a pandas timedelta string or a pandas Timedelta object.")

    total_minutes = timedelta_obj.total_seconds() // 60
    if hours:
        _hours = int(total_minutes // 60)
    _minutes = int(total_minutes % 60)

    if ignore_zero:
        _tuple = (_hours, _minutes)
        _tuple = (v if v > 0 else '' for v in _tuple)
        _hours, _minutes = _tuple

    return f"{_hours}:{_minutes}"


def timedelta_to_str(time_delta: timedelta, hours: bool = True, minutes: bool = True, seconds: bool = False,
                     milliseconds: bool = False, microseconds: bool = False, ignore_zero: bool = True) -> str:
    """
        Convert a pandas timedelta string or a pandas Timedelta object into a human-readable string representation.

        This function takes a pandas timedelta string, a pandas Timedelta object, or a datetime.timedelta object
        and converts it into a string format of hours, minutes, seconds, milliseconds, and/or microseconds.
        If the input is a string, it is converted to a Timedelta object. The resulting string represents the time
        components specified by the function parameters.

        Parameters:
            time_delta (timedelta): The input timedelta, which should be a timedelta.
            hours (bool, optional): If True (default), includes hours in the output. If False, excludes hours.
            minutes (bool, optional): If True (default), includes minutes in the output. If False, excludes minutes.
            seconds (bool, optional): If True, includes seconds in the output. Default is False.
            milliseconds (bool, optional): If True, includes milliseconds in the output. Default is False.
            microseconds (bool, optional): If True, includes microseconds in the output. Default is False.
            ignore_zero (bool, optional): If True (default), removes zero values from the output.

        Returns:
            str: A string representation of the input timedelta in the specified format "hours:minutes:seconds:milliseconds".

        Raises:
            ValueError: If the input is not a pandas timedelta string, a pandas Timedelta object, or a datetime.timedelta object.

        Example:
            # Convert a timedelta string to a human-readable string
            time_str = "2 days 03:30:45.123456"
            result = timedelta_to_str(time_str, hours=True, minutes=True, seconds=True, milliseconds=True, microseconds=True)
            # Result: "51:30:45:123456"

            # Convert a Timedelta object to a human-readable string
            import pandas as pd
            time_delta = pd.Timedelta(days=2, hours=3, minutes=30, seconds=45, milliseconds=123, microseconds=456)
            result = timedelta_to_str(time_delta, hours=True, minutes=True, seconds=True, milliseconds=True, microseconds=True)
            # Result: "51:30:45:123456"
        """
    _hours, _minutes, _seconds, _seconds_fraction = [''] * 4
    remained_seconds = time_delta.total_seconds()
    if hours:
        _hours = int(remained_seconds // 60 * 60)
        remained_seconds -= _hours * 60 * 60
    if minutes:
        _minutes = int(remained_seconds // 60)
        remained_seconds -= _minutes * 60
    if seconds:
        _seconds = int(remained_seconds // 1)
        remained_seconds -= _seconds
    if microseconds:
        _seconds_fraction = int(remained_seconds // 0.000001) * 0.000001
    elif milliseconds:
        _seconds_fraction = int(remained_seconds // 0.001) * 0.001
        # remained_seconds -= _milliseconds
    if ignore_zero:
        _tuple = (_hours, _minutes, _seconds, _seconds_fraction)
        _tuple = tuple([v if (v == '' or v > 0) else '' for v in _tuple])
        _hours, _minutes, _seconds, _seconds_fraction = _tuple
    result = f'{_hours}:{_minutes}:{_seconds}:{_seconds_fraction}'
    return result


def read_with_timeframe(data_frame_type: str, date_range_str: str, file_path: str, n_rows: int,
                        skip_rows: int) -> pd.DataFrame:
    """
    Read data from a compressed CSV file, adjusting the index based on the data frame type.

    This function reads data from a compressed CSV file based on the specified data frame type and date range.
    It adjusts the index of the resulting DataFrame according to the data frame type. If the data frame type
    includes 'multi_timeframe', it sets the index with both 'timeframe' and 'date' levels and swaps them.

    Parameters:
        data_frame_type (str): The type of data frame being read, such as 'ohlcv', 'ohlcva', or 'multi_timeframe_ohlcva'.
        date_range_str (str): The date range string used to generate the file name.
        file_path (str): The path to the directory containing the data file.
        n_rows (int): The maximum number of rows to read from the CSV file.
        skip_rows (int): The number of rows to skip at the beginning of the CSV file.

    Returns:
        pd.DataFrame: The DataFrame containing the read data with adjusted index.

    Example:
        # Read OHLC data with adjusted index
        ohlcv_data = read_with_timeframe('ohlcv', '21-07-01.00-00T21-07-02', '/path/to/data/', n_rows=1000, skip_rows=0)
    """
    if date_range_str is None:
        date_range_str = config.under_process_date_range
    df = pd.read_csv(os.path.join(file_path, f'{data_frame_type}.{date_range_str}.zip'), sep=',', header=0,
                     index_col='date', parse_dates=['date'], skiprows=skip_rows, nrows=n_rows)
    if 'multi_timeframe' in data_frame_type:
        df.set_index('timeframe', append=True, inplace=True)
        df = df.swaplevel()
    return df


# def zz_check_dataframe(dataframe: pd.DataFrame, columns: [str], raise_exception=False):
#     try:
#         dataframe.columns
#     except NameError:
#         if raise_exception:
#             raise Exception(
#                 f'The DataFrame does not have columns:{dataframe}')
#         else:
#             return False
#     for _column in columns:
#         if _column not in list(dataframe.columns) + list(dataframe.index.names):
#             if raise_exception:
#                 raise Exception(
#                     f'The DataFrame expected to contain {_column} but have these columns:{dataframe.columns}')
#             else:
#                 return False
#     return True


# @measure_time
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


@measure_time
def to_timeframe(time: Union[DatetimeIndex, datetime], timeframe: str, ignore_cached_times: bool = False) -> datetime:
    """
    Round the given datetime to the nearest time based on the specified timeframe.

    Parameters:
        time (datetime): The datetime to be rounded.
        timeframe (str): The desired timeframe (e.g., '1min', '5min', '1H', etc.).

    Returns:
        datetime: The rounded datetime that corresponds to the nearest time within the specified timeframe.
        :param ignore_cached_times:
    """
    # if time == Timestamp("2023-11-12 00:00:00+00:00") and timeframe == '1W':
    #     pass
    # Calculate the timedelta for the specified timeframe
    timeframe_timedelta = pd.to_timedelta(timeframe)

    # Calculate the number of seconds in the timedelta
    seconds_in_timeframe = timeframe_timedelta.total_seconds()
    if pd.to_timedelta(timeframe) >= timedelta(minutes=30):
        if time.tzinfo is None:
            raise Exception('To round times to timeframes > 30 minutes timezone is significant')
    assert not isinstance(time, DatetimeIndex)

    if isinstance(time, datetime) or isinstance(time, Timestamp):
        if pd.to_timedelta(timeframe) >= timedelta(days=7):
            rounded_time = time.replace(hour=0, minute=0, second=0, microsecond=0)
            day_of_week = time.weekday()  # (time.day_of_week + 1) % 7
            rounded_time = rounded_time - timedelta(days=day_of_week)
        else:
            rounded_timestamp = (time.timestamp() // seconds_in_timeframe) * seconds_in_timeframe
            # Convert the rounded timestamp back to datetime
            if isinstance(time, datetime):
                rounded_time = time.fromtimestamp(rounded_timestamp, tz=time.tzinfo)
            else:  # isinstance(time, Timestamp)
                rounded_time = pd.Timestamp(rounded_timestamp * 10 ** 9, tz=time.tzinfo)
        if not ignore_cached_times:
            if f'valid_times_{timeframe}' not in GLOBAL_CACHE.keys():
                raise Exception(f'valid_times_{timeframe} not initialized in GLOBAL_CACHE')
            if rounded_time not in GLOBAL_CACHE[f'valid_times_{timeframe}']:
                raise Exception(f'time {rounded_time} not found in GLOBAL_CACHE[valid_times_{timeframe}]!')
    else:
        raise Exception(f'Invalid type of time:{type(time)}')
    # if tz is not None:
    #     # if isinstance(rounded_time, datetime):
    #     #     rounded_time = rounded_time.replace(tzinfo=tz)
    #     if isinstance(rounded_time, Timestamp):
    #         rounded_time = rounded_time.tz_localize(tz)
    assert abs(rounded_time - time) < timeframe_timedelta
    return rounded_time


def zz_test_index_match_timeframe(data: pd.DataFrame, timeframe: str):
    for index_value, mapped_index_value in map(lambda x, y: (x, y), data.index, to_timeframe(data.index, timeframe)):
        if index_value != mapped_index_value:
            raise Exception(
                f'In Data({data.columns.names}) found Index({index_value}) not align with timeframe:{timeframe}/{mapped_index_value}\n'
                f'Indexes:{data.index.values}')


def validate_no_timeframe(data: pd.DataFrame) -> pd.DataFrame:
    if 'timeframe' in data.index.names:
        raise Exception(f'timeframe found in Data(indexes:{data.index.names}, columns:{data.columns.names}')
    return data


@measure_time
def times_tester(df: pd.DataFrame, date_range_str: str, timeframe: str, return_bool: bool = False):
    expected_times = set(times_in_date_range(date_range_str, timeframe).tz_localize(None))
    actual_times = set(df.index.tz_localize(tz=None))

    # Checking if all expected times are in the dataframe's index
    missing_times = expected_times - actual_times
    if len(missing_times) == 0:
        return True
    else:
        if return_bool:
            return False
        else:
            raise Exception("Some times from the date range are missing in the DataFrame's index:" +
                            ', '.join([str(time) for time in missing_times]))


@measure_time
def multi_timeframe_times_tester(multi_timeframe_df: pt.DataFrame[MultiTimeframe], date_range_str: str,
                                 return_bool: bool = False):
    result = True
    for timeframe in config.timeframes:
        _timeframe_df = single_timeframe(multi_timeframe_df, timeframe)
        result = result & times_tester(_timeframe_df, date_range_str, timeframe, return_bool)
    return result


def expected_movement_size(_list: List):
    return _list  # * CandleSize.Standard.value[0]


def shift_time(timeframe, shifter):
    index = config.timeframes.index(timeframe)
    if type(shifter) == int:
        return config.timeframes[index + shifter]
    elif type(shifter) == str:
        if shifter not in config.timeframe_shifter.keys():
            raise Exception(f'Shifter expected be in [{config.timeframe_shifter.keys()}]')
        return config.timeframes[index + config.timeframe_shifter[shifter]]
    else:
        raise Exception(f'shifter expected be int or str got type({type(shifter)}) in {shifter}')


from collections import ChainMap


def all_annotations(cls) -> ChainMap:
    """Returns a dictionary-like ChainMap that includes annotations for all
       attributes defined in cls or inherited from superclasses."""
    all_classes_list = [c.__annotations__ for c in cls.__mro__ if hasattr(c, '__annotations__')]
    annotations = {}
    for single_class_annotations in all_classes_list:
        for attr_name, attr_type in single_class_annotations.items():
            if attr_name not in ['date', 'timeframe', 'Config'] and '__' not in attr_name:
                annotations[attr_name] = attr_type
    return annotations  # ChainMap(*(c.__annotations__ for c in cls.__mro__ if '__annotations__' in c.__dict__))


def cast_and_validate(data, model_class: Type[Pandera_DFM_Type], return_bool: bool = False,
                      zero_size_allowed: bool = False) -> Union[Pandera_DFM_Type, bool]:
    if len(data) == 0:
        if not zero_size_allowed:
            raise Exception('Zero size data!')
        else:
            if return_bool:
                return True
            else:
                return pd.DataFrame()
    as_types = {}
    _all_annotations = all_annotations(model_class)

    for attr_name, attr_type in _all_annotations.items():
        if 'timestamp' in str(attr_type).lower() and 'timestamp' not in str(data.dtypes.loc[attr_name]).lower():
            as_types[attr_name] = 'datetime64[s]'
        elif 'timedelta' in str(attr_type).lower() and 'timedelta' not in str(data.dtypes.loc[attr_name]).lower():
            as_types[attr_name] = 'timedelta64[s]'
            # as_types[attr_name] = pandera.typing.Timedelta
        elif 'pandera.typing.pandas.Series' in str(attr_type):
            astype = str(attr_type).replace('pandera.typing.pandas.Series[', '').replace(']', '')
            trans_table = str.maketrans('', '', string.digits)
            astype = astype.translate(trans_table)
            if (astype != 'str' and
                    attr_name in data.columns and astype not in str(data.dtypes.loc[attr_name]).lower()):
                as_types[attr_name] = astype
    if len(as_types) > 0:
        data = data.astype(as_types)
    if return_bool:
        try:
            model_class.validate(data, lazy=True)
        except pandera.errors.SchemaErrors as exc:
            log(exc.schema_errors)
            return False
        except Exception as e:
            raise e
    else:
        model_class.validate(data, lazy=True, )
    if return_bool:
        return True
    data = data[[column for column in model_class.__fields__.keys() if column not in ['timeframe', 'date']]]
    return data


def trigger_timeframe(timeframe):
    if config.timeframes.index(timeframe) < -config.timeframe_shifter['trigger']:
        raise Exception(f'{timeframe} has not a trigger time!')
    return shift_time(timeframe, config.timeframe_shifter['trigger'])


def pattern_timeframe(timeframe):
    if config.timeframes.index(timeframe) < -config.timeframe_shifter['pattern']:
        raise Exception(f'{timeframe} has not a pattern time!')
    return shift_time(timeframe, config.timeframe_shifter['pattern'])


def anti_pattern_timeframe(timeframe):
    if config.timeframes.index(timeframe) > len(config.timeframes) + config.timeframe_shifter['pattern'] - 1:
        raise Exception(f'{timeframe} has not an anit pattern time!')
    return shift_time(timeframe, -config.timeframe_shifter['pattern'])


def anti_trigger_timeframe(timeframe):
    if config.timeframes.index(timeframe) > len(config.timeframes) + config.timeframe_shifter['trigger'] - 1:
        raise Exception(f'{timeframe} has not an anit trigger time!')
    return shift_time(timeframe, -config.timeframe_shifter['trigger'])


def map_symbol(symbol: str, map_dictionary: dict) -> str:
    upper_symbol = symbol.upper()
    if upper_symbol in map_dictionary.values():
        return symbol.upper()
    return map_dictionary[upper_symbol]


@dataclass
class FileInfoSet:
    symbol: str
    file_type: str
    date_range: str


def extract_file_info(file_name: str) -> FileInfoSet:
    pattern = re.compile(r'^((?P<symbol>[\w]+)\.)?(?P<file_type>[\w_]+)\.(?P<date_range>[\d\-\.T]+)\.zip$')
    match = pattern.match(file_name)
    if not match or len(match.groupdict()) < 3:
        raise Exception("Invalid filename format:" + file_name)
    data = match.groupdict()
    if 'symbol' not in data.keys() or data['symbol'] is None:
        data['symbol'] = config.under_process_symbol
    return FileInfoSet(**data)


# @cache
def trim_to_date_range(date_range_str: str, df: pd.DataFrame) -> pd.DataFrame:
    start, end = date_range(date_range_str)
    date_indexes = df.index.get_level_values(level='date')
    df = df[
        (date_indexes >= np.datetime64(start)) &
        (date_indexes <= np.datetime64(end))
        ]
    assert not df.index.duplicated().any()
    return df


def expand_date_range(date_range_str: str, time_delta: timedelta, mode: str):
    start, end = date_range(date_range_str)
    if mode == 'start':
        start = start - time_delta
    elif mode == 'end':
        end = end + time_delta
    elif mode == 'both':
        start = start - time_delta
        end = end + time_delta
    else:
        raise Exception(f'mode={mode} not implemented')
    return date_range_to_string(start=start, end=end)


def after_under_process_date(date_range_str):
    start, _ = date_range(date_range_str)
    _, end = date_range(config.under_process_date_range)
    if start > end:
        allow_zero_size = True
    else:
        allow_zero_size = False
    return allow_zero_size


@measure_time
def times_in_date_range(date_range_str: str, timeframe: str) -> DatetimeIndex:
    start_date, end_date = date_range(date_range_str)
    in_timeframe_start_date = to_timeframe(start_date, timeframe, ignore_cached_times=True)
    if in_timeframe_start_date < start_date:
        in_timeframe_start_date += pd.to_timedelta(timeframe)
    if timeframe == '1W':
        frequency = 'W-MON'
    elif timeframe == 'M':
        frequency = 'MS'
    else:
        frequency = timeframe
    return pd.date_range(start=in_timeframe_start_date, end=end_date, freq=frequency)
