from enum import Enum

class Events(Enum):
    new = 1
    update = 2
    delete = 3
    commit = 4
    preflush = 5
    postflush = 6
