from dtypes import Stop, StopTime, Corridor, ServiceTypes, TripTimetable

from dataclasses import dataclass
from typing import Optional, Union
from itertools import chain
import functools

@dataclass(repr=False)
class CorridorOps:
    corridor: Corridor

    #TODO:
    #       - Add a filter for the settlement    
    #       - A Timetable should just be for a single trip_id
    #       - Add stop_sequence to the timetable


if __name__ == "__main__":
    pass