from helper import measure_time


def read_multi_timeframe_color_trend_pivots(date_range_str: str = None):
    raise Exception('Not implemented!')


@measure_time
def generate_multi_timeframe_color_trend_pivots():
    """
        trend pivots:
            find any sequence of same color candles
            marge same color boundaries separated with a reverse color trend < 1 ATR
                each one with movement >= 3 ATR
                    follwed with a reverse color trend >= 1 ATR
    :return:
    """
    # todo: implement  generate_multi_timeframe_same_color_trend_pivots
    raise Exception('Not implemented')
