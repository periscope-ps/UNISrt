import enum

class types(enum.Enum):
    CREATE = enum.auto()
    ADD = enum.auto()
    COMMIT = enum.auto()
    CHANGE = enum.auto()
    PULL = enum.auto()
    DELETE = enum.auto()
    PREPUSH = enum.auto()
    POSTPUSH = enum.auto()
    DATA = enum.auto()
