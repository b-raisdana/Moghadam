import re
import string
from dataclasses import dataclass
from datetime import datetime, timedelta
from typing import Callable, Union, List, Type, Any, Dict
from zipfile import BadZipFile

import numpy as np
import pandas as pd
import pandera
import pytz
from pandas import Timedelta, DatetimeIndex, Timestamp
from pandas._typing import Axes
from pandera import typing as pt, DataType

from Config import config
from PanderaDFM.MultiTimeframe import MultiTimeframe_Type, MultiTimeframe
from helper.helper import log, date_range, date_range_to_string, morning, Pandera_DFM_Type, LogSeverity, log_d


def date_range_of_data(data: pd.DataFrame) -> str:
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


# @cache
def datarange_is_not_cachable(date_range_str):
    _, end = date_range(date_range_str)
    if end > morning(datetime.utcnow().replace(tzinfo=pytz.UTC)):
        return True
    return False


def no_generator(*args, **kwargs):
    raise Exception("There is no Generator and expected to reading be successful ever.")


def read_file(date_range_str: str, data_frame_type: str, generator: Callable, caster_model: Type[Pandera_DFM_Type]
              , skip_rows=None, n_rows=None, file_path: str = config.path_of_data,
              zero_size_allowed: Union[None, bool] = None) -> pd.DataFrame:
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
        date_range_str = config.processing_date_range
    df = None
    try:
        df = read_with_timeframe(data_frame_type, date_range_str, file_path, n_rows, skip_rows)
    except FileNotFoundError as e:
        pass
    except Exception as e:
        raise e

    if zero_size_allowed is None:
        zero_size_allowed = after_under_process_date(date_range_str)
    if df is None or not cast_and_validate(df, caster_model, return_bool=True, zero_size_allowed=zero_size_allowed):
        try:
            generator(date_range_str)
        except Exception as e:
            # recovers the missing raised exception
            raise e
        df = read_with_timeframe(data_frame_type, date_range_str, file_path, n_rows, skip_rows)
        df = cast_and_validate(df, caster_model, zero_size_allowed=zero_size_allowed)
    else:
        df = cast_and_validate(df, caster_model, zero_size_allowed=zero_size_allowed)
    if datarange_is_not_cachable(date_range_str):
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
    elif isinstance(input_time, float):
        timedelta_obj = timedelta(seconds=input_time)
    else:
        raise ValueError(
            "Input should be either a pandas timedelta string, float(seconds) or a pandas Timedelta object.")

    total_minutes = timedelta_obj.total_seconds() // 60
    _hours = 0
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


import os
import pandas as pd


def read_with_timeframe(data_frame_type: str, date_range_str: str, file_path: str, n_rows: int,
                        skip_rows: int) -> pd.DataFrame:
    """
    Read data from a compressed CSV file, adjusting the index based on the data frame type.

    This function reads data from a compressed CSV file based on the specified data frame type and date range.
    It adjusts the index of the resulting DataFrame according to the data frame type. If the data frame type
    includes 'multi_timeframe', it sets the index with both 'timeframe' and 'date' levels and swaps them.
    The 'date' index is assumed to be UTC.

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
        date_range_str = config.processing_date_range
    file_name = os.path.join(file_path, f'{data_frame_type}.{date_range_str}.zip')
    try:
        df = pd.read_csv(file_name, sep=',', header=0,
                         index_col='date', parse_dates=['date'], skiprows=skip_rows, nrows=n_rows)
    except BadZipFile:
        raise Exception(f'{file_name} is not a zip file!')

    # Convert the 'date' index to UTC if it's timezone-unaware
    if len(df) > 0:
        if not hasattr(df.index, 'tz'):
            pass
        if df.index.tz is None:
            df.index = df.index.tz_localize('UTC')

    if 'multi_timeframe' in data_frame_type:
        df = df.set_index('timeframe', append=True, )
        df = df.swaplevel()

    return df


# @measure_time
def single_timeframe(multi_timeframe_data: pt.DataFrame[MultiTimeframe_Type], timeframe) -> pd.DataFrame:
    if 'timeframe' not in multi_timeframe_data.index.names:
        raise Exception(
            f'multi_timeframe_data expected to have "timeframe" in indexes:[{multi_timeframe_data.index.names}]')
    if timeframe not in config.timeframes:
        raise Exception(
            f'timeframe:{timeframe} is not in supported timeframes:{config.timeframes}')
    single_timeframe_data: pd.DataFrame = multi_timeframe_data.loc[
        multi_timeframe_data.index.get_level_values('timeframe') == timeframe]
    return validate_no_timeframe(single_timeframe_data.droplevel('timeframe'))


def to_timeframe(time: Union[DatetimeIndex, datetime, Timestamp], timeframe: str, ignore_cached_times: bool = False) \
        -> Union[datetime, DatetimeIndex]:
    """
    Round down the given datetime or DatetimeIndex to the nearest time based on the specified timeframe.

    This function adjusts a datetime or each datetime in a DatetimeIndex to align with the start of a specified timeframe, such as '1min', '5min', '1H', etc. It is particularly useful for aligning timestamps to regular intervals.

    Parameters:
        time (Union[DatetimeIndex, datetime, Timestamp]): The datetime or DatetimeIndex to be rounded.
        timeframe (str): The desired timeframe to round to (e.g., '1min', '5min', '1H', etc.).
        ignore_cached_times (bool): If True, bypasses checking the time against a global cache of valid times.

    Returns:
        Union[datetime, DatetimeIndex]: The rounded datetime or DatetimeIndex, where each datetime value is adjusted to the start of the nearest interval as specified by the timeframe.

    Raises:
        Exception: If time types are incompatible, or if rounding requirements are not met.
    """

    def round_single_datetime(dt: Union[datetime, Timestamp]):
        """
        Function to round a single datetime
        :param dt:
        :return:
        """

        if pd.to_timedelta(timeframe) >= timedelta(days=7):
            rounded_dt = dt.replace(hour=0, minute=0, second=0, microsecond=0)
            day_of_week = dt.weekday()
            rounded_dt = rounded_dt - timedelta(days=day_of_week)
        else:
            rounded_timestamp = (dt.timestamp() // seconds_in_timeframe) * seconds_in_timeframe
            if isinstance(dt, datetime):
                rounded_dt = datetime.fromtimestamp(rounded_timestamp, tz=dt.tzinfo)
            else:  # isinstance(dt, Timestamp)
                rounded_dt = pd.Timestamp(rounded_timestamp * 10 ** 9, tz=dt.tzinfo)
        return rounded_dt

    # Calculate the timedelta for the specified timeframe
    timeframe_timedelta = pd.to_timedelta(timeframe)
    seconds_in_timeframe = timeframe_timedelta.total_seconds()

    if pd.to_timedelta(timeframe) >= timedelta(minutes=30):
        if getattr(time, 'tzinfo', None) is None:
            raise Exception('To round times to timeframes > 30 minutes, timezone is significant')
    if isinstance(time, (datetime, Timestamp)):
        rounded_time = round_single_datetime(time)
    elif isinstance(time, DatetimeIndex):
        rounded_time = time.to_series().apply(round_single_datetime)
    else:
        raise Exception(f'Invalid type of time: {type(time)}')

    if not ignore_cached_times:
        check_time_in_cache(rounded_time, timeframe)

    return rounded_time


def check_time_in_cache(time, timeframe):
    cache_key = f'valid_times_{timeframe}'
    if cache_key not in config.GLOBAL_CACHE.keys():
        raise Exception(f'{cache_key} not initialized in config.GLOBAL_CACHE')
    if isinstance(time, DatetimeIndex) or isinstance(time, pd.Series):
        if not time.isin(config.GLOBAL_CACHE[cache_key]).all():
            raise Exception(f'Some times: {time} not found in config.GLOBAL_CACHE[valid_times_{timeframe}]!')
    elif time not in config.GLOBAL_CACHE[cache_key]:
        raise Exception(f'time {time} not found in config.GLOBAL_CACHE[valid_times_{timeframe}]!')


# def zz_test_index_match_timeframe(data: pd.DataFrame, timeframe: str):
#     for index_value, mapped_index_value in map(lambda x, y: (x, y), data.index, to_timeframe(data.index, timeframe)):
#         if index_value != mapped_index_value:
#             raise Exception(
#                 f'In Data({data.columns.names}) found Index({index_value}) not align with timeframe:{timeframe}/{mapped_index_value}\n'
#                 f'Indexes:{data.index.values}')


def validate_no_timeframe(data: pd.DataFrame) -> pd.DataFrame:
    if 'timeframe' in data.index.names:
        raise Exception(f'timeframe found in Data(indexes:{data.index.names}, columns:{data.columns.names}')
    return data


def times_tester(df: pd.DataFrame, date_range_str: str, timeframe: str, return_bool: bool = False,
                 limit_to_under_process_period: bool = True,
                 processing_date_range: str = None,
                 exact_match: bool = False,
                 ) -> Union[bool, None]:
    expected_times = set(times_in_date_range(date_range_str, timeframe, limit_to_under_process_period,
                                             processing_date_range))
    if len(expected_times) == 0:
        return True
    if len(df.index) > 0:
        actual_times = set(df.index)
    else:
        actual_times = set()

    # Checking if all expected times are in the dataframe's index
    missing_times = expected_times - actual_times
    if missing_times:
        message = (f"Some times in {date_range_str}@{timeframe} are missing in the DataFrame's index:" +
                   ', '.join([str(time) for time in missing_times]))
        if return_bool:
            log(message)
            return False
        else:
            raise Exception(message)
    else:
        if exact_match:
            excess_times = actual_times - expected_times
            if excess_times == 0:
                return True
            else:
                message = (f"Some times in {date_range_str}@{timeframe} are excessive in the DataFrame's index:" +
                           ', '.join([str(time) for time in excess_times]))
                if return_bool:
                    log(message)
                    return False
                else:
                    raise Exception(message)
        else:
            return True


def multi_timeframe_times_tester(multi_timeframe_df: pt.DataFrame[MultiTimeframe], date_range_str: str,
                                 return_bool: bool = False, ignore_processing_date_range: bool = True,
                                 processing_date_range: str = None):
    result = True
    for timeframe in config.timeframes:
        _timeframe_df = single_timeframe(multi_timeframe_df, timeframe)
        try:
            result = result & times_tester(_timeframe_df, date_range_str, timeframe, return_bool,
                                           ignore_processing_date_range, processing_date_range)
        except Exception as e:
            raise e
    return result


def expected_movement_size(_list: List):
    return _list  # * CandleSize.Standard.value.min


def shift_timeframe(timeframe, shifter):
    index = config.timeframes.index(timeframe)
    if type(shifter) == int:
        return config.timeframes[index + shifter]
    elif type(shifter) == str:
        if shifter not in config.timeframe_shifter.keys():
            raise Exception(f'Shifter expected be in [{config.timeframe_shifter.keys()}]')
        return config.timeframes[index + config.timeframe_shifter[shifter]]
    else:
        raise Exception(f'shifter expected be int or str got type({type(shifter)}) in {shifter}')


def all_annotations(cls, include_indexes=False) -> dict:
    """Returns a dictionary-like ChainMap that includes annotations for all
       attributes defined in cls or inherited from superclasses."""
    all_classes_list = [c.__annotations__ for c in cls.__mro__ if hasattr(c, '__annotations__')]
    annotations = {}
    if include_indexes:
        drop_list = ['Config']
    else:
        drop_list = ['date', 'timeframe', 'Config']
    for single_class_annotations in all_classes_list:
        for attr_name, attr_type in single_class_annotations.items():
            if attr_name not in drop_list and '__' not in attr_name:
                annotations[attr_name] = attr_type
    return annotations  # ChainMap(*(c.__annotations__ for c in cls.__mro__ if '__annotations__' in c.__dict__))


def cast_and_validate(data, model_class: Type[Pandera_DFM_Type], return_bool: bool = False,
                      zero_size_allowed: bool = False, unique_index: bool = False) -> Any:
    if len(data) == 0:
        if not zero_size_allowed:
            raise Exception('Zero size data!')
        else:
            if return_bool:
                return True
            else:
                return empty_df(model_class)
    if unique_index:
        if not data.index.is_unique:
            log("Not tested", severity=LogSeverity.ERROR)
            raise Exception(f"Expected to be unique but found duplicates:{data.index[data.index.duplicated()]}")
    try:
        data = apply_as_type(data, model_class)
    except KeyError as e:
        if return_bool:
            log_d(e)
            return False
        else:
            raise e
    if return_bool:
        try:
            model_class.validate(data, lazy=True)
        except pandera.errors.SchemaErrors as exc:
            log(str(exc.schema_errors), LogSeverity.WARNING, stack_trace=False)
            return False
        except Exception as e:
            raise e
    else:
        try:
            model_class.validate(data, lazy=True, )
        except Exception as e:
            raise e
    if return_bool:
        return True
    try:
        columns_to_keep: list[str] = [column for column in model_class.__fields__.keys() if
                                      column not in ['timeframe', 'date']]
    except Exception as e:
        raise e
    try:
        data = data[columns_to_keep]
    except Exception as e:
        raise e
    return data


def apply_as_type(data, model_class) -> pd.DataFrame:
    as_types = {}
    _all_annotations = all_annotations(model_class)
    for attr_name, attr_type in _all_annotations.items():
        if (attr_name not in data.dtypes.keys()
                and (hasattr(data.index, 'names') and attr_name not in data.index.names)):
            raise KeyError(f"'{attr_name}' in {model_class.__name__} but not in data:{data.dtypes}")
        try:
            if 'timestamp' in str(attr_type).lower() and 'timestamp' not in str(data.dtypes.loc[attr_name]).lower():
                as_types[attr_name] = 'datetime64[ns, UTC]'
            if 'datetimetzdtype' in str(attr_type).lower():

                if 'datetimetzdtype' not in str(data.dtypes.loc[attr_name]).lower():
                    as_types[attr_name] = 'datetime64[ns, UTC]'
                elif 'timedelta' in str(attr_type).lower() and 'timedelta' not in str(
                        data.dtypes.loc[attr_name]).lower():
                    as_types[attr_name] = 'timedelta64[s]'
                    # as_types[attr_name] = pandera.typing.Timedelta
            elif 'pandera.typing.pandas.Series' in str(attr_type):
                astype = str(attr_type).replace('pandera.typing.pandas.Series[', '').replace(']', '')
                trans_table = str.maketrans('', '', string.digits)
                astype = astype.translate(trans_table)
                if (astype != 'str' and
                        attr_name in data.columns and astype not in str(data.dtypes.loc[attr_name]).lower()):
                    as_types[attr_name] = astype
        except Exception as e:
            raise e
    if len(as_types) > 0:
        # log(as_types)
        try:
            data = data.astype(as_types)
        except Exception as e:
            raise e
    return data


def cast_and_validate2(data, model_class: Type[Pandera_DFM_Type], return_bool: bool = False,
                       zero_size_allowed: bool = False, unique_index: bool = False) -> Any:
    if len(data) == 0:
        if not zero_size_allowed:
            raise Exception('Zero size data!')
        else:
            if return_bool:
                return True
            else:
                return empty_df(model_class)
    if unique_index:
        if not data.index.is_unique:
            log("Not tested", severity=LogSeverity.ERROR)
            raise Exception(f"Expected to be unique but found duplicates:{data.index[data.index.duplicated()]}")
    try:
        column_annotations = column_dtypes(data, model_class)
        data = apply_as_type2(data, model_class, column_annotations)
    except KeyError as e:
        if return_bool:
            log_d(e)
            return False
        else:
            raise e
    if return_bool:
        try:
            model_class.validate(data, lazy=True)
        except pandera.errors.SchemaErrors as exc:
            log(str(exc.schema_errors), LogSeverity.WARNING, stack_trace=False)
            return False
        except Exception as e:
            raise e
    else:
        try:
            model_class.validate(data, lazy=True, )
        except Exception as e:
            raise e
    if return_bool:
        return True
    # try:
    #     columns_to_keep: list[str] = [column for column in model_class.__fields__.keys() if
    #                                   column not in ['timeframe', 'date']]
    # except Exception as e:
    #     raise e
    try:
        data = data[column_annotations.keys()]
    except Exception as e:
        raise e
    return data


def apply_as_type2(data, model_class, _column_dtypes) -> pd.DataFrame:
    as_types = {}
    for attr_name, attr_type in _column_dtypes.items():
        if (attr_name not in data.dtypes.keys()
                and (hasattr(data.index, 'names') and attr_name not in data.index.names)):
            raise KeyError(f"'{attr_name}' in {model_class.__name__} but not in data:{data.dtypes}")
        try:
            if 'timestamp' in str(attr_type).lower() and 'timestamp' not in str(data.dtypes.loc[attr_name]).lower():
                as_types[attr_name] = 'datetime64[ns, UTC]'
            if 'datetimetzdtype' in str(attr_type).lower():

                if 'datetimetzdtype' not in str(data.dtypes.loc[attr_name]).lower():
                    as_types[attr_name] = 'datetime64[ns, UTC]'
                elif 'timedelta' in str(attr_type).lower() and 'timedelta' not in str(
                        data.dtypes.loc[attr_name]).lower():
                    as_types[attr_name] = 'timedelta64[s]'
                    # as_types[attr_name] = pandera.typing.Timedelta
            elif 'pandera.typing.pandas.Series' in str(attr_type):
                astype = str(attr_type).replace('pandera.typing.pandas.Series[', '').replace(']', '')
                trans_table = str.maketrans('', '', string.digits)
                astype = astype.translate(trans_table)
                if (astype != 'str' and
                        attr_name in data.columns and astype not in str(data.dtypes.loc[attr_name]).lower()):
                    as_types[attr_name] = astype
        except Exception as e:
            raise e
    if len(as_types) > 0:
        # log(as_types)
        try:
            data = data.astype(as_types)
        except Exception as e:
            raise e
    return data


def column_dtypes(data, model_class) -> Dict[str, DataType]:
    _all_annotations = all_annotations(model_class)
    data_index_names = index_names(data)
    column_annotations = {k: a for k, a in _all_annotations.items() if k not in data_index_names}
    d_type: str
    return column_annotations


def index_names(data):
    _index_names = []
    if hasattr(data.index, 'names'):
        _index_names += data.index.names
    elif hasattr(data.index, 'name'):
        if data.index.name is None or data.index.name == "":
            raise Exception('Set name of index as title!')
        _index_names = [data.index.name]
    return _index_names


def trigger_timeframe(timeframe):
    if config.timeframes.index(timeframe) < -config.timeframe_shifter['trigger']:
        raise Exception(f'{timeframe} has not a trigger time!')
    return shift_timeframe(timeframe, config.timeframe_shifter['trigger'])


def pattern_timeframe(timeframe):
    if config.timeframes.index(timeframe) < -config.timeframe_shifter['pattern']:
        raise Exception(f'{timeframe} has not a pattern time!')
    return shift_timeframe(timeframe, config.timeframe_shifter['pattern'])


def anti_pattern_timeframe(timeframe):
    if config.timeframes.index(timeframe) > len(config.timeframes) + config.timeframe_shifter['pattern'] - 1:
        raise Exception(f'{timeframe} has not an anit-pattern time!')
    return shift_timeframe(timeframe, -config.timeframe_shifter['pattern'])


def anti_trigger_timeframe(timeframe):
    if config.timeframes.index(timeframe) > len(config.timeframes) + config.timeframe_shifter['trigger'] - 1:
        raise Exception(f'{timeframe} has not an anti-trigger time!')
    return shift_timeframe(timeframe, -config.timeframe_shifter['trigger'])


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
def trim_to_date_range(date_range_str: str, df: pd.DataFrame, ignore_duplicate_index: bool = False) -> pd.DataFrame:
    start, end = date_range(date_range_str)
    date_indexes = df.index.get_level_values(level='date')
    df = df[
        (date_indexes >= start) &
        (date_indexes <= end)
        ]
    duplicate_indices = df.index[df.index.duplicated()].unique()
    if not ignore_duplicate_index:
        assert len(duplicate_indices) == 0
    # else:
    #     if len(duplicate_indices) > 0:
    #         log(f"Found duplicate indices:" + str(duplicate_indices))
    return df


def expand_date_range(date_range_str: str, time_delta: timedelta, mode: str, limit_to_processing_period: bool = None) \
        -> str:
    if limit_to_processing_period is None:
        limit_to_processing_period = config.limit_to_under_process_period
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
    if limit_to_processing_period:
        _, processing_period_end = date_range(config.processing_date_range)
        end = min(end, processing_period_end)
    return date_range_to_string(start=start, end=end)


def after_under_process_date(date_range_str):
    start, _ = date_range(date_range_str)
    _, end = date_range(config.processing_date_range)
    if start > end:
        allow_zero_size = True
    else:
        allow_zero_size = False
    return allow_zero_size


def times_in_date_range(date_range_str: str, timeframe: str,
                        ignore_out_of_process_period: bool = True,
                        processing_date_range: str = None) -> DatetimeIndex:
    start, end = date_range(date_range_str)
    if ignore_out_of_process_period:
        if processing_date_range is None:
            processing_date_range = config.processing_date_range
        under_process_scope_start, under_process_scope_end = date_range(processing_date_range)
        end = min(end, under_process_scope_end)
        start = max(start, under_process_scope_start)
    in_timeframe_start_date = to_timeframe(start, timeframe, ignore_cached_times=True)
    if start < end:
        if in_timeframe_start_date < start:
            in_timeframe_start_date += pd.to_timedelta(timeframe)
        if timeframe == '1W':
            frequency = 'W-MON'
        elif timeframe == 'M':
            frequency = 'MS'
        else:
            frequency = timeframe
        return pd.date_range(start=in_timeframe_start_date, end=end, freq=frequency)
    return pd.DatetimeIndex([], tz=pytz.utc)


def index_fields(model_class: Type[Pandera_DFM_Type]) -> dict[str, str]:
    if 'PeakValleys' in model_class.__name__:
        pass
    if hasattr(model_class.to_schema().index, 'columns'):
        # model_class has a MultiIndex
        # names = list(model_class.to_schema().index.columns.keys())
        names = model_class.to_schema().index.dtypes
    else:
        # model_class has a single Index
        all_fields = all_annotations(model_class, include_indexes=True)
        names = {k: model_class.to_schema().index.dtype for k, v in all_fields.items()
                 if 'pandera.typing.pandas.Index' in str(v.__origin__)}
    return names


def column_fields(model_class: Type[Pandera_DFM_Type]) -> dict[str, DataType]:
    return model_class.to_schema().dtypes
    # return list(model_class.to_schema().columns.keys())


def empty_df(model_class: Type[Pandera_DFM_Type]) -> pd.DataFrame:
    as_types = dict(column_fields(model_class), **index_fields(model_class))
    # Create an empty DataFrame with Pandas-compatible data types
    empty_data = {
        column: [] for column in as_types.keys()
    }
    _empty_df = pd.DataFrame(empty_data)
    for _name, _type in as_types.items():
        as_types[_name] = _type.type.name

    _empty_df = _empty_df.astype(as_types)
    # if len(index_fields(model_class).keys()) == 0:
    #     pass
    _empty_df = _empty_df.set_index(list(index_fields(model_class).keys()))
    _empty_df = model_class(_empty_df)
    return _empty_df


def shift_over(needles: Axes, reference: Axes, side: str, start=None, end=None) -> Axes:
    """
    it will merge the indexes for both forward and backward and return. missing indexes in forward will be filled
    forward and missing indexes of backward will be filled backward.\n
    to find adjacent or nearest PREVIOUS row we can use as:\n
    mapped_list = shift_over(needles, reference, 'forward')\n
    to find adjacent or nearest NEXT row we can use as:\n
    mapped_list = shift_over(needles, reference, 'backward')

    :param needles: needles list
    :param reference: reference list
    :param start: filtering the start of output indexes
    :param end: filtering the rnd of output indexes
    :param side: should be either "forward" or "backward". use "forward" to get the PREVIOUS reference for needles and
    use "backward" to get the NEXT reference for needles
    :return: Axes indexed as the combination of forward and backward indexes and 2 columns:
        'forward': forward input mapped to indexes and forward filled.
        'backward': backward input mapped to indexes and backward filled.

    Example:\n
    reference.index	    1 2 3 5 10 20       \n
    needles.index    	1 2 3 6 9 15 20     \n

    shift_over(needles, reference, 'forward'):
    mapped_list.index   1  2 3 5 6 9  10 15 20      \n
    forward(reference)  NA 1 2 3 5 5  5  10 10      \n
    backward(needles)   2  3 6 6 9 15 15 20 NA      \n
    return:
                        1  2 3 6 9 15 20 \n
                        NA 1 2 5 5 10 10

    shift_over(needles, reference, 'backward'):
    mapped_list.index   1  2 3  5  6  9 10 15 20
    forward(needles)    NA 1 2  3  3  6  9  9 15
    backward(reference) 2  3 5 10 10 10 20 20 NA
    return:
                        1 2 3  6  9 15 20   \n
                        2 3 5 10 10 20 NA
    """
    # Todo: replace with pd.merge_asof
    side = side.lower()
    if side == 'forward':
        forward = reference
        backward = needles
    elif side == 'backward':
        forward = needles
        backward = reference
    else:
        raise Exception('side should be either "forward" or "backward".')
    df = pd.DataFrame(index=forward.append(backward).unique())
    df = df.sort_index()
    if side == 'forward':
        df.loc[forward, 'forward'] = forward
        df['forward'] = df['forward'].ffill().shift(1)
    elif side == 'backward':
        df.loc[backward, 'backward'] = backward
        df['backward'] = df['backward'].bfill().shift(-1)
    if start is not None:
        df = df[df.index >= start]
    if end is not None:
        df = df[df.index >= end]
    if side == 'forward':
        return df.loc[needles, 'forward'].to_list()
    elif side == 'backward':
        return df.loc[needles, 'backward'].to_list()


def concat(left: pd.DataFrame, right: pd.DataFrame):
    if not left.empty and not left.isna().all().all():
        if not right.empty and not right.isna().all().all():
            if left.isna().all(axis=0).any():
                pass
            if right.isna().all(axis=0).any():
                pass
            right_na_columns = right.dtypes[right.isna().all()]
            # right_na_column_dtypes = right.dtypes[right_na_columns]
            left_na_columns = left.dtypes[left.isna().all()]
            # left_na_column_dtypes = left.dtypes[left_na_columns]
            left = pd.concat([left.dropna(axis=1, how='all'), right.dropna(axis=1, how='all')])
            for column, d_type in left_na_columns.items():
                left[column] = pd.Series(dtype=d_type)
            for column, d_type in right_na_columns.items():
                if column not in left.columns:
                    left[column] = pd.Series(dtype=d_type)
    else:
        if not right.empty and not right.isna().all().all():
            left = right.copy()
        else:
            left = pd.DataFrame()
    return left
