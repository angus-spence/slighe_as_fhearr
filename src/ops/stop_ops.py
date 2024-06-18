import dtypes
import transforms

from typing import Optional, Union
from itertools import chain

def uclid_distance(stops: list[dtypes.Stop]) -> float: return NotImplementedError

def _get_stop_times(stop: dtypes.Stop,
                    service_types: list[dtypes.ServiceTypes],
                    routes: list[dtypes.Route]
                    ) -> list[dtypes.StopTime]:
    return list(chain.from_iterable(list(chain.from_iterable([[[trip.stop_times[i] for i in range(len(trip.stop_times)) if trip.stop_times[i].stop_id == stop.stop_id and trip.service_id in [st.value for st in service_types]] for trip in route.trips] for route in routes]))))

def _frequency(stop_times: list[dtypes.StopTime],
               start: Optional[Union[str, float]],
               end: Optional[Union[str, float]]
               ) -> int:
    if isinstance(start, str): start = transforms.ts_to_float(start, "%H:%M:%S")
    if isinstance(end, str): end = transforms.ts_to_float(end, "%H:%M:%S")
    [stop_time.timestring_to_float() for stop_time in stop_times if isinstance(stop_time.arrival_time, str) or isinstance(stop_time.departure_time, str)]
    return len([stop_time for stop_time in stop_times if stop_time.arrival_time > start and stop_time.arrival_time < end])

def frequency(stop: dtypes.Stop,
              service_type: list[dtypes.ServiceTypes],
              routes: list[dtypes.Route],
              start: Optional[Union[str, float]],
              end: Optional[Union[str, float]]
              ) -> int:
    """
    """
    return _frequency(_get_stop_times(stop, service_type, routes), start, end)

if __name__ == "__main__":
    import constructors, dload
    loader = dload.GTFSLoadCSV('./data/agency.csv', './data/calendar.csv', './data/calendar_dates.csv', './data/routes.csv', './data/stop_times.csv', './data/stops.csv', './data/trips.csv')
    corridor = constructors.RouteConstructor(['2991_37732', '2990_40267', '3038_40330'], loader).build()
    stop = dtypes.Stop(stop_id='852000011', stop_name='Post Office', stop_latitude='54.19213076', stop_longitude='-7.704834099', settlement='Swanlinbar', county='County_Cavan')
    print(frequency(stop, [dtypes.ServiceTypes.MON], [corridor[0]], "07:00:00", "23:00:00"))