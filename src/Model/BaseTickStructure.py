from dataclasses import dataclass
from datetime import datetime


@dataclass
class BaseTickStructure:
    date: datetime
    open: float
    close: float
    high: float
    low: float
    volume: float
