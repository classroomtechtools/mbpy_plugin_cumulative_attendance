from enum import Enum
from datetime import timedelta


class WorkWeekEnum(Enum):
    MON_FRI = "mon-fri"
    SUN_THURS = "sun-thurs"


class WorkWeek:
    def __init__(self, type: str):
        self.type = WorkWeekEnum(type)

    def first_day_of_week(self, day):
        if self.type == WorkWeekEnum.MON_FRI:
            days = day.weekday()
        elif self.type == WorkWeekEnum.SUN_THURS:
            days = (day.weekday() + 1) % 7

        return day - timedelta(days=days)

    @property
    def weekends(self):
        return {WorkWeekEnum.MON_FRI: (5, 6), WorkWeekEnum.SUN_THURS: (4, 5)}.get(self.type)
