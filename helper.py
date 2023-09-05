import datetime
import functools
import time
import traceback
from enum import Enum


class LogSeverity(Enum):
    WARNING = 'Warning'


def log(log_message, severity=LogSeverity.WARNING, stack_trace: bool = True) -> None:
    print(f'{severity.value}@{datetime.datetime.now().strftime("%m-%d.%H:%M:%S")}#{log_message}')
    if stack_trace:
        stack = traceback.extract_stack(limit=2 + 1)[:-1]  # Remove the last item
        traceback.print_list(stack)


def measure_time(func):
    @functools.wraps(func)
    def _measure_time(*args, **kwargs):
        start_time = time.time()
        log(f"{func.__name__} started", stack_trace=False)
        result = func(*args, **kwargs)
        end_time = time.time()
        execution_time = end_time - start_time
        log(f"{func.__name__} executed in {execution_time:.6f} seconds", stack_trace=False)
        return result

    return _measure_time
