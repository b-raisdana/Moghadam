from sqlite3 import Timestamp

from pandera import typing as pt

from Model.BullBearSide import BaseBullBearSide


class MultiTimeframeBullBearSide(BaseBullBearSide):
    timeframe: pt.Index[str]
    date: pt.Index[Timestamp]  # start