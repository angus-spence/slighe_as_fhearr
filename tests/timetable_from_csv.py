from dtypes import Corridor
from dload import GTFSLoadCSV
from constructors import CorridorConstructor, CorrdidorTimetableConstructor

import csv

def main(corridor_csv: str) -> ...:
    corridors = _get_corridors(corridor_csv)
    _build_timetables(corridors)

def _build_timetables(corridors: dict) -> list[Corridor]:
    loader = GTFSLoadCSV('./data/agency.csv', 
                         './data/calendar.csv', 
                         './data/calendar_dates.csv', 
                         './data/routes.csv', 
                         './data/stop_times.csv', 
                         './data/stops.csv', 
                         './data/trips.csv')
    for corridor_id, routes in corridors.items():
        corridor = CorridorConstructor(corridor_id, "", routes, loader).build()
        corridor_timetable = CorrdidorTimetableConstructor(corridor).build()
        corridor_timetable.sort_by_time()
        corridor_timetable.disolve_stops()
        if len(corridor_timetable.timetable) < 1: continue
        else: corridor_timetable.to_csv(f'./tests/outputs/{corridor.corridor_id}_timetable.csv')

def _get_corridors(corridor_csv) -> dict:
    with open(corridor_csv, "r", encoding='utf_8_sig') as f:
        reader = csv.DictReader(f)
        data: list[dict] = []
        for row in reader:
            data.append(row)
        corridor_ids = list(set([list(row.values())[1] for row in data]))
        corridors = dict(zip(corridor_ids, [[0] for _ in corridor_ids]))
        for row in data:
            corridors[row['corridor_id']].append(row['route_id'])
    return corridors

if __name__ == "__main__":
    main('data\corridor.csv')