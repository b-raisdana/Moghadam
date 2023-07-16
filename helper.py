import datetime
from enum import Enum


class LogSeverity(Enum):
    WARNING = 'Warning'


def log(log_message, severity=LogSeverity.WARNING):
    print(f'{severity}@{datetime.datetime.now().strftime("%y-%m-%dT%H:%M:%S")}:{log_message}')
