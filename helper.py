import datetime
import functools
import time
import traceback
from datetime import datetime, timedelta
from enum import Enum
from typing import Tuple

import pytz


class LogSeverity(Enum):
    INFO = "INFO"
    WARNING = "WARNING"
    ERROR = "ERROR"


def log(log_message: str, severity: LogSeverity = LogSeverity.WARNING, stack_trace: bool = True) -> None:
    """
    Log a message with an optional severity level and stack trace.

    Args:
        log_message (str): The message to be logged.
        severity (LogSeverity, optional): The severity level of the log message. Defaults to LogSeverity.WARNING.
        stack_trace (bool, optional): Whether to include a stack trace in the log message. Defaults to True.

    Returns:
        None
    """
    print(f'{severity.value}@{datetime.now().strftime("%m-%d.%H:%M:%S")}#{log_message}')
    if stack_trace:
        stack = traceback.extract_stack(limit=2 + 1)[:-1]  # Remove the last item
        traceback.print_list(stack)


def measure_time(func):
    @functools.wraps(func)
    def _measure_time(*args, **kwargs):
        """
        Measure the execution time of a function and log the start and end times.

        Args:
            *args: Positional arguments to be passed to the wrapped function.
            **kwargs: Keyword arguments to be passed to the wrapped function.

        Returns:
            result: The result of the wrapped function.
        """
        start_time = time.time()
        log(f"{func.__name__} started", stack_trace=False)
        result = func(*args, **kwargs)
        end_time = time.time()
        execution_time = end_time - start_time
        log(f"{func.__name__} executed in {execution_time:.6f} seconds", stack_trace=False)
        return result

    return _measure_time


def date_range(date_range_str: str) -> Tuple[datetime, datetime]:
    start_date_string, end_date_string = date_range_str.split('T')
    start_date = datetime.strptime(start_date_string, '%y-%m-%d.%H-%M')
    end_date = datetime.strptime(end_date_string, '%y-%m-%d.%H-%M')
    return start_date, end_date


def date_range_to_string(end_date: datetime = None, days: float = 60, start_date: datetime = None) -> str:
    if end_date is None:
        end_date = today_morning()
    if start_date is None:
        start_date = end_date - timedelta(days=days) + timedelta(minutes=1)
        return f'{start_date.strftime("%y-%m-%d.%H-%M")}T' \
               f'{end_date.strftime("%y-%m-%d.%H-%M")}'
    else:
        return f'{start_date.strftime("%y-%m-%d.%H-%M")}T' \
               f'{end_date.strftime("%y-%m-%d.%H-%M")}'


def today_morning(tz=pytz.timezone('Asia/Tehran')) -> datetime:
    return morning(datetime.now(tz)) - timedelta(minutes=1)


def morning(date_time: datetime, tz=pytz.timezone('Asia/Tehran')):
    # return tz.localize(datetime.combine(date_time.date(), time(0, 0)), is_dst=None)
    if date_time.tzinfo is None or date_time.tzinfo.utcoffset(date_time) is None:
        date_time = tz.localize(date_time, is_dst=None)
    return date_time.replace(hour=0, minute=0, second=0)
