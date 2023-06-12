# from datetime import timedelta
# from pydantic import BaseModel, validator
# from pandas.tseries.frequencies import to_offset
#
#
# class CandleTime(BaseModel):
#     time_delta: timedelta
#     '''
#     to check frequency is valid:
#
#     from pandas.tseries.frequencies import to_offset
#         to_offset("5min")
#         to_offset("1D1H")
#         to_offset("2W")
#         to_offset("3X")
#     '''
#
#     frequency: str
#
#     @classmethod
#     @validator('frequency')
#     def _valid_frequency(cls, v):
#         try:
#             to_offset(v)
#         except Exception as e:
#             raise ValueError('frequency is not valid!')

pass
# todo: remove if not used!