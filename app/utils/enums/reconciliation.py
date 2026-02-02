from enum import IntEnum


class ReconciliationStatus(IntEnum):
    PENDING = 0
    IN_PROGRESS = 1
    COMPLETED = 2
    EXCEPTION = 3
    CANCELLED = 4
