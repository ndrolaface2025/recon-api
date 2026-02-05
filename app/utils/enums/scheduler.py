from enum import IntEnum


class SchedulerStatus(IntEnum):
    IN_PROGRESS = 0
    SUCCESS = 1
    PARTIAL = 2
    FAILED = 3
