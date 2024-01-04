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

    def __str__(self):
        return (f"{self.date.strftime('%y-%m-%d.%H-%M')}:{self.open:.2f},{self.high:.2f},"
                f"{self.low:.2f},{self.close:.2f}"
                f"/{self.volume:.2f}")
