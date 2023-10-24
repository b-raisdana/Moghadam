from datetime import datetime

import pandas as pd
from pandas import to_timedelta
from pandera import typing as pt

from BullBearSide import bull_bear_side_repr
from Model.BullBearSide import BullBearSide
from Config import TREND
from helper import log


# def zz_merge_overlapped_single_timeframe_trends(trends: pt.DataFrame[BullBearSide], timeframe: str):
#     """
#     Merge overlapped trends.
#
#     If 2 trends of the same kind (BULLISH, BEARISH, SIDE already defined in TRENDS enum and compare values) overlapped
#     (have at least one candle in common) merge them.
#     If a SIDE trend overlaps with a BULLISH or BEARISH trend, we move the start (index) and end of SIDE trend to remove
#     overlap.
#     If a SIDE trend is completely covered by a BULLISH or BEARISH trend, remove the SIDE trend.
#     Just the start (index) of BULLISH/BEARISH trends can overlap with end of another BULLISH/BEARISH of the reverse
#     type. Else: raise exception.
#
#     :param timeframe:
#     :param trends: DataFrame containing trend data.
#                       Columns: ['end', 'direction']
#                       Index: 'start' timestamp of the trend
#     :return: Merged and cleaned trends DataFrame.
#     """
#     # todo: test merge_overlapped_trends
#     # merged_trends = pd.DataFrame()
#     trends.sort_index(inplace=True, level='date')
#     for _idx in range(len(trends) - 1):
#         start = trends.index[_idx]
#         trend = trends.iloc[_idx]
#         end = trend['end']
#         direction = trend['bull_bear_side']
#         rest_of_trends = trends.loc[(trends.index != start) | ((trends.index == start) & (trends['end'] != end))]
#         # Find overlapping trends of the same kind
#         same_kind_overlaps = rest_of_trends[
#             (rest_of_trends.index <= end) & (rest_of_trends['end'] >= start) & (
#                         rest_of_trends['bull_bear_side'] == direction)
#             ]
#
#         trends_to_drop = []
#
#         # Merge overlapping trends of the same kind into one trend
#         if len(same_kind_overlaps) > 0:
#             """
#             date: pt.Index[datetime]  # start
#             bull_bear_side: pt.Series[str]
#             end: pt.Series[datetime]
#             internal_high: pt.Series[float]
#             high_time: pt.Series[Timestamp]
#             internal_low: pt.Series[float]
#             low_time: pt.Series[Timestamp]
#             movement: pt.Series[float]
#             strength: pt.Series[float]
#             ATR: pt.Series[float]
#             duration: pt.Series[timedelta]
#             """
#             same_kind_overlaps.loc[start] = trend
#             trends_to_merge = same_kind_overlaps.sort_index()
#             start_of_merged = trends_to_merge.index[0]
#             end_of_merged = trends_to_merge['end'].max()
#             bull_bear_side_of_merged = direction
#             internal_high = trends_to_merge['internal_high'].max()
#             time_of_highest = trends_to_merge['internal_high'].idxmax()
#             high_time = trends_to_merge.loc[time_of_highest, 'high_time']
#             internal_low = trends_to_merge['internal_low'].min()
#             low_time = trends_to_merge.loc[trends_to_merge['low_time'].idxmin(), 'low_time']
#             movement = internal_high - internal_low
#             duration = end_of_merged - start_of_merged
#             strength = movement / (duration / to_timedelta(timeframe))
#             atr = sum(trends_to_merge['ATR'] / trends_to_merge['duration'])
#             merged_trend = pd.Series({
#                 'end': end_of_merged,
#                 'bull_bear_side': bull_bear_side_of_merged,
#                 'end': end_of_merged,
#                 'internal_high': internal_high,
#                 'high_time': high_time,
#                 'internal_low': internal_low,
#                 'low_time': low_time,
#                 'movement': movement,
#                 'strength': strength,
#                 'ATR': atr,
#                 'duration': duration,
#             })
#             trends.loc[start_of_merged] = merged_trend
#             trends_to_drop.append(trends_to_merge.index)
#     trends.drop(list(set(trends_to_drop)))
#     for _idx in range(len(trends) - 1):
#         start = trends.index[_idx]
#         trend = trends.iloc[_idx]
#         direction = trend['bull_bear_side']
#         rest_of_trends = trends.loc[(trends.index != start) | ((trends.index == start) & (trends['end'] != end))]
#         if direction == TREND.SIDE.value:
#
#             # Check if a SIDE trend overlaps with a BULLISH or BEARISH trend
#             new_start, new_end = start, trend['end']
#             start_overlapping_bull_bears = rest_of_trends[
#                 (rest_of_trends.index <= start)
#                 & (rest_of_trends['end'] > start)
#                 # & (trends_df['bull_bear_side'].isin([TREND.BULLISH.value, TREND.BEARISH.value]))
#                 ]
#             if len(start_overlapping_bull_bears) > 0:
#                 # make sure none of SIDE trends overlaps with other SIDE trends
#                 test_side_trend_not_overlapping_another_side(start, trend, start_overlapping_bull_bears)
#                 new_start = start_overlapping_bull_bears['end'].max() + to_timedelta(timeframe)
#                 trends.rename({start: new_start}, inplace=True)
#
#             end_overlapping_bull_bear = rest_of_trends[
#                 (rest_of_trends.index <= trend['end'])
#                 & (rest_of_trends['end'] > trend['end'])
#                 # & (trends_df['bull_bear_side'].isin([TREND.BULLISH.value, TREND.BEARISH.value]))
#                 ]
#
#             if len(end_overlapping_bull_bear) > 0:
#                 # make sure none of SIDE trends overlaps with other SIDE trends
#                 test_side_trend_not_overlapping_another_side(start, trend, end_overlapping_bull_bear)
#                 new_end = end_overlapping_bull_bear.index.min() - to_timedelta(timeframe)
#                 # trends.loc[(trends.index == start)&(trends['end']==trend['end']), 'end'] = new_end
#                 trends.iloc[_idx]['end'] = new_end
#
#             # drop trend if end before start
#             if new_end <= new_start:
#                 log(f'{bull_bear_side_repr(start, trend)} removed as covered by: '
#                     f'{[bull_bear_side_repr(o_start, o_trend) for o_start, o_trend in start_overlapping_bull_bears.iterrows()]}')
#                 trends.drop(new_start)
#                 continue
#
#             # drop SIDE trends completely covered by a BULLISH or BEARISH trend
#             covering_trends = rest_of_trends[(trends.index <= start) & (trends['end'] >= new_end)]
#             if len(covering_trends) > 0:
#                 trends.drop(new_start)
#                 continue
#
#     return trends
#
#     # merged_trends = []
#     #
#     # for start, trend in trends_df.iterrows():
#     #     end = trend['end']
#     #     direction = trend['bull_bear_side']
#     #
#     #     # Find overlapping trends
#     #     overlaps = trends_df[
#     #         (trends_df.index <= end) & (trends_df['end'] >= start) & (trends_df['bull_bear_side'] == direction)
#     #         ]
#     #
#     #     # Check for reverse direction overlaps
#     #     reverse_direction = TREND.BULLISH.value if direction == TREND.BEARISH.value else TREND.BEARISH.value
#     #     reverse_overlaps = trends_df[
#     #         (trends_df.index <= end) & (trends_df['end'] >= start) & (trends_df['bull_bear_side'] == reverse_direction)
#     #         ]
#     #
#     #     # Check if any overlapping trend covers the current trend
#     #
#     #     if len(reverse_overlaps) > 0:
#     #         _message = f'{trend["bull_bear_side"].replace("_TREND", "")}: ' \
#     #                    f'{start.strftime("%H:%M")}-{trend["end"].strftime("%H:%M")}' \
#     #                    f" Reverse direction overlap with "
#     #         for r_start, r_trend in reverse_overlaps.iterrows():
#     #             _message += f'{r_trend["bull_bear_side"].replace("_TREND", "")}: ' \
#     #                         f'{r_start.strftime("%H:%M")}-{r_trend["end"].strftime("%H:%M")}'
#     #         log(_message)
#     #         # raise Exception(_message )
#     #
#     #     # Check if any BULL/BEAR trend covers SIDE trends
#     #     if direction == 'SIDE':
#     #         covering_bull_bear = overlaps[overlaps['bull_bear_side'].isin([TREND.BULLISH.value, TREND.BEARISH.value])]
#     #         if len(covering_bull_bear) > 0:
#     #             continue  # Skip this SIDE trend
#     #
#     #     # Merge overlapping trends
#     #     if len(overlaps) > 1:
#     #         end = max(overlaps['end'].max(), end)
#     #
#     #     merged_trends.append({'start': start, 'end': end, 'bull_bear_side': direction})
#     #
#     # merged_trends_df = pd.DataFrame(merged_trends)
#     # return merged_trends_df


def test_bull_bear_side_trends(trends: pt.DataFrame[BullBearSide]):
    for start, trend in trends.iterrows():
        end = trend['end']
        direction = trend['bull_bear_side']

        trend_overlaps = trends[
            (trends.index <= end) & (trends['end'] >= start)
            ]
        if len(trend_overlaps) > 0:
            # none of trends should overlap with another trend of same direction
            if direction in trend_overlaps['bull_bear_side']:
                _msg = f"{bull_bear_side_repr(start, trend)} overlaps with same direction trends:"
                _msg = str.join([bull_bear_side_repr(o_start, o_trend) for o_start, o_trend in
                                 trend_overlaps[trend_overlaps['bull_bear_side'] == direction].iterrows()])
                raise Exception(_msg)
            if TREND.SIDE.value in trend_overlaps['bull_bear_side']:
                _msg = f"{bull_bear_side_repr(start, trend)} overlaps with SIDE trends:"
                _msg = str.join([bull_bear_side_repr(o_start, o_trend) for o_start, o_trend in
                                 trend_overlaps[trend_overlaps['bull_bear_side'] == direction].iterrows()])
                raise Exception(_msg)
            # at this point we are sure the trend only overlaps with a reverse BULL/BEAR trend
            start_overlaps = trends[
                (trends.index <= end) & (trends.index >= start)
                ]
            end_overlaps = trends[
                (trends['end'] <= end) & (trends['end'] >= start)
                ]
            if len(start_overlaps) > 0:
                if len(start_overlaps) > 1:
                    raise Exception(f"{bull_bear_side_repr(start, trend)} have more than one start overlaps:"
                                    f"{[bull_bear_side_repr(o_start, o_trend) for o_start, o_trend in start_overlaps]}")
                if start != start_overlaps.iloc[0]['end']:
                    raise Exception(f"start of {bull_bear_side_repr(start, trend)} does not match with end of "
                                    f"{bull_bear_side_repr(start_overlaps.index[0], start_overlaps.iloc[0])}")
            if len(end_overlaps) > 0:
                if len(end_overlaps) > 1:
                    raise Exception(f"{bull_bear_side_repr(start, trend)} have more than one end overlaps:"
                                    f"{[bull_bear_side_repr(o_start, o_trend) for o_start, o_trend in end_overlaps]}")
                if trend['end'] != end_overlaps.index[0]:
                    raise Exception(f"end of {bull_bear_side_repr(start, trend)} does not match with start of "
                                    f"{bull_bear_side_repr(end_overlaps.index[0], end_overlaps.iloc[0])}")


def test_side_trend_not_overlapping_another_side(start: datetime, trend: pd.Series,
                                                 overlapping_trends: pt.DataFrame[BullBearSide],
                                                 raise_exception: bool = False):
    if TREND.SIDE.value in overlapping_trends['bull_bear_side'].unique():
        _message = f'{bull_bear_side_repr(start, trend)} overlaps with"'
        for o_start, o_trend in overlapping_trends[overlapping_trends['bull_bear_side'] == TREND.SIDE.value]:
            _message += bull_bear_side_repr(o_start, o_trend)
        if raise_exception:
            raise Exception(_message)
        else:
            log(_message)
            return False
    return True
