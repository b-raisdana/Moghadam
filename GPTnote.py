'''
this files used as note for
change following code to every time we run this:
1. find gaps which we do not have OHLCV data for last 1 year period up to today morning 00:00:00.
2. fetch all the
'''


'''
A.Rename the existing read_multi_timeframe_ohlcv to old_ read_multi_timeframe_ohlcv
B.update read_multi_timeframe_ohlcv(date_range_str, presice_start_date = false, precise_end_date = false) with a date_range_str like ‘22-08-09.00-01T23-08-09.00-00’ 
    1.	start, end = date_range(date_range_str: str)
    2.	split date range from start to end day by day and for eachd day:
        a.	create read_daily_multi_timeframe_ohlcv(day: datetime, timezone = GMT)-> padera.DataFrame [MultiTimeframeOHLCV]
        b.	assign day_daterange as YY-MM-DD.00-00TYY-MM-DD.23-59 for day.
        c.	Read old_ read_multi_timeframe_ohlcv(date_range_str)
    3.	Concatenate daily multi_timeframe_ohlcv and return
ttt
'''

from Config import config, GLOBAL_CACHE
from DataPreparation import read_file, single_timeframe
from pandera import typing as pt

def date_range(date_range_str: str) -> Tuple[datetime, datetime]:
    start_date_string, end_date_string = date_range_str.split('T')
    start_date = datetime.strptime(start_date_string, '%y-%m-%d.%H-%M')
    end_date = datetime.strptime(end_date_string, '%y-%m-%d.%H-%M')
    return start_date, end_date


def read_multi_timeframe_ohlcv(date_range_str: str = None) \
        -> pt.DataFrame[MultiTimeframeOHLCV]:
    result = read_file(date_range_str, 'multi_timeframe_ohlcv', generate_multi_timeframe_ohlcv,
                       MultiTimeframeOHLCV)
    for timeframe in config.timeframes:
        GLOBAL_CACHE[f'valid_times_{timeframe}'] = \
            single_timeframe(result, timeframe).index.get_level_values('date').tolist()
    return result
