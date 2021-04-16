from enum import Enum

class Events(Enum):
    new = 1
    internalupdate = 2
    update = 3
    delete = 4
    commit = 5
    preflush = 6
    postflush = 7
    data = 8
