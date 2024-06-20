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
    # for corridor_id, routes in corridors.items():
    for corridor_id, routes in [(x, corridors.get(x)) for x in ['445']]: # To try - A31(time error, above 24hours)
    # Tried - B52, 445
    # for corridor_id, routes in [('445', corridors.get('445'))]:
    # corridors = {corridor_id:{0, route_ids, ...}}
        corridor = CorridorConstructor(corridor_id, "", routes, loader).build()
        corridor_timetable = CorrdidorTimetableConstructor(corridor).build()
        corridor_timetable.sort_by_time()
        if len(corridor_timetable.timetable) < 1: continue
        else: corridor_timetable.to_csv(f'./tests/outputs/{corridor.corridor_id}_timetable_duplicated.csv')
        corridor_timetable.disolve_stops()
        if len(corridor_timetable.timetable) < 1: pass #continue #? Why would corridor_timetable.timetable be empty? route_id not present in routes.csv
        else: corridor_timetable.to_csv(f'./tests/outputs/{corridor.corridor_id}_timetable_disolved.csv')

def _get_corridors(corridor_csv) -> dict:
    with open(corridor_csv, "r", encoding='utf_8_sig') as f:
        reader = csv.DictReader(f) #Read f from csv into python dict 
        data: list[dict] = []
        for row in reader:
            data.append(row) # data = ({"r_id":"r_id_num", "c_id":"c_id_num"}, {...}, ...)
        corridor_ids = list(set([list(row.values())[1] for row in data])) # Find the uniqlue c_id and into a list
        corridors = dict(zip(corridor_ids, [[0] for _ in corridor_ids])) # corridors = {"c_di1": ?[0]?, 'c_id2': ???...}
        # Get 0 away but still create a list
        for row in data:
            corridors[row['corridor_id']].append(row['route_id']) # corridors = {"c_di1": (?[0]?, r_id1_c_id1, r_id2_c_id1, r_id3_c_id1), 'c_id2': ???...}
    return corridors

if __name__ == "__main__":
    main('data\corridor.csv')