from enum import Enum
from dataclasses import dataclass, field
from itertools import chain
from typing import Union, Generator
import csv
import time

import pandas as pd
import os
from geographiclib.geodesic import Geodesic

@dataclass
class NTATimeTable:
    stop_times_file_path: str
    trips_file_path: str
    routes_file_path: str
    stops_file_path: str
    calendar_file_path: str
    routes_corridors_file_path: str
    settlements_filter_file_path: str
    output_directory: str

    def __post_init__(self) -> None:
        print(f'{os.get_terminal_size().columns * "_"}\nNTA DATA AGGREGATOR')
        self._make_out_dir(); self._load_to_pandas(); self._merge_dataframes() 
        
    def _make_out_dir(self) -> None:
        if not os.path.exists(os.path.join(os.getcwd(), self.output_directory)): 
            os.mkdir(os.path.join(os.getcwd(), self.output_directory))

    def _load_to_pandas(self) -> tuple[pd.DataFrame]:
        print(f'--> LOADING DATA TO PANDAS DATAFRAME')
        self.stop_times_df = pd.read_csv(self.stop_times_file_path, header=0, dtype={'trip_id': str, 'stop_id': str, 'stop_sequence': int, 'arrival_time': str, 'departure_time': str}, encoding='latin_1')
        self.trips_df = pd.read_csv(self.trips_file_path)
        self.routes_df = pd.read_csv(self.routes_file_path)
        self.stops_df = pd.read_csv(self.stops_file_path, encoding='latin_1')
        self.calendar_df = pd.read_csv(self.calendar_file_path)
        self.routes_corridors_df = pd.read_csv(self.routes_corridors_file_path)
        self.settlements_filter_df = pd.read_csv(self.settlements_filter_file_path)
        return self.stop_times_df, self.trips_df, self.routes_df, self.stops_df, self.calendar_df, self.routes_corridors_df

    def _merge_dataframes(self) -> tuple[pd.DataFrame]:
        """
        Merges the trips dataframe with the calandar and routes dataframe to get service_type and route_short_name
        """
        print(f'--> MERGING DATAFRAME OBJECTS')
        self.trips_df = self.trips_df.merge(self.calendar_df[['service_id', 'service_type']], on='service_id')
        self.trips_df = self.trips_df.merge(self.routes_df[['route_id', 'route_short_name']], on='route_id')
        self.routes_df = self.routes_df.merge(self.routes_corridors_df[['route_id', 'corridor_id']], on='route_id')
        return self.trips_df, self.routes_df
    

    def _convert_to_sortable_datetime(self, time_str) -> pd.DataFrame:
        """
        """
        if pd.isnull(time_str) or time_str == '0': return pd.NaT  # NaT represents Not-a-Time    
        return pd.to_datetime(time_str, format='%H:%M:%S', errors='coerce')

    def sort_dataframe(self, df) -> pd.DataFrame:
        """
        """
        for col in df.columns[4:]:
            df[col] = df[col].apply(self._convert_to_sortable_datetime)

            def first_valid_value(column):
                non_nan_values = df[column].dropna()
                first_valid = non_nan_values.first_valid_index()
                return non_nan_values.at[first_valid] if first_valid is not None else pd.Timestamp.max

        sorted_trip_columns = sorted(df.columns[4:], key=first_valid_value)
        return df[['direction_id', 'stop_id', 'stop_sequence', 'stop_name'] + sorted_trip_columns]

    def _evaluate_direction(df: pd.DataFrame, 
                            start_latitude_column: Union[str, int], 
                            start_longitude_column: Union[str, int], 
                            end_latitude_column: Union[str, int],
                            end_longitude_column: Union[str, int],
                            css: ...
                            ) -> pd.DataFrame:
        
        return Geodesic.Inverse(start_latitude_column,
                                      start_longitude_column,
                                      end_latitude_column,
                                      end_longitude_column)['azi1']

    def _build_timetable_for_corridor(self, corridor_id) -> ...:
        """
        
        """
        corridor_routes: pd.DataFrame = self.routes_df[self.routes_df['corridor_id'] == corridor_id]
        corridor_timetable = pd.DataFrame()

        for _, route in corridor_routes.iterrows():
            route_id = route['route_id']
            route_trips: pd.DataFrame = self.trips_df[self.trips_df['route_id'] == route_id]

            stop_times_route = pd.merge(
                self.stop_times_df,
                route_trips[['trip_id', 'route_id', 'direction_id', 'service_type', 'route_short_name']],
                on='trip_id'
            )

            stop_times_route = pd.merge(
                stop_times_route,
                self.stops_df[['stop_id', 'stop_name']]
            )

            corridor_timetable = pd.concat([corridor_timetable, stop_times_route], ignore_index=True, sort=False)

        corridor_timetable = corridor_timetable.drop_duplicates(subset=['direction_id', 'stop_id', 'stop_sequence', 'arrival_time'])

        # Pivot the table to have trip_ids as columns and stop details as rows
        corridor_timetable_pivot = corridor_timetable.pivot_table(
            index=['direction_id', 'stop_id', 'stop_sequence', 'stop_name'],
            columns=['service_type', 'trip_id', 'route_short_name'],
            values='arrival_time',
            aggfunc='first'
        )

        # Reset index to turn the pivot table back into a DataFrame
        corridor_timetable_reset = corridor_timetable_pivot.reset_index()

        # Sort by direction and then by stop_sequence
        corridor_timetable_sorted = corridor_timetable_reset.sort_values(by=['direction_id', 'stop_sequence'])

        # Create custom headers with service_type, route_short_name, and ascending order
        new_headers = ['direction_id', 'stop_id', 'stop_sequence', 'stop_name']  # Initial headers
        order_counters = {}
        for col in corridor_timetable_sorted.columns[len(new_headers):]:
            service_type, trip_id, route_short_name = col
            order_counters.setdefault((service_type, route_short_name), 0)
            order_counters[(service_type, route_short_name)] += 1
            new_header = f"{service_type}_{route_short_name}_{order_counters[(service_type, route_short_name)]}"
            new_headers.append(new_header)

        corridor_timetable_sorted.columns = new_headers

        # Split by direction_id and sort each dataframe
        df1 = self.sort_dataframe(corridor_timetable_sorted)
        df2 = self.sort_dataframe(corridor_timetable_sorted)

        def time_filter(df: pd.DataFrame):
            """
            Takes a dataframe and converts all datetime elements into time, without date

            Parameters
            ----------
            df: pd.DataFrame
                Pandas DataFrame to convert
            """

            def format_time(datatime_obj):
                if pd.isna(datatime_obj):
                    return None
                return datatime_obj.strftime('%H:%M:%S')

            for col in df.columns[4:]:
                df[col] = df[col].apply(format_time)

            return df

        # Re-merge the data
        corridor_timetable_merged = pd.concat([df1, df2])
        corridor_timetable_merged = time_filter(corridor_timetable_merged)

        corridor_timetable_merged = pd.concat([df1, df2])
        corridor_timetable_merged = time_filter(corridor_timetable_merged)

        stops = self.stops_df[['stop_id', 'stop_lat', 'stop_lon', 'settlement', 'county']]
        corridor_timetable_merged = corridor_timetable_merged.merge(stops, left_on='stop_id', right_on='stop_id')
        new_cols = list(corridor_timetable_merged.columns.values)[-2:]
        poped = corridor_timetable_merged[new_cols]

        corridor_timetable_merged = corridor_timetable_merged.drop(columns=poped)
        corridor_timetable_merged.insert(4, 'settlement', poped.iloc[:,0])
        corridor_timetable_merged.insert(4, 'county', poped.iloc[:,1])

        # Sort by direction and then by stop_sequence
        corridor_timetable_merged = corridor_timetable_merged.sort_values(by=['direction_id', 'stop_sequence'])
        corridor_timetable_merged = corridor_timetable_merged.drop_duplicates(subset=['direction_id', 'stop_id', 'stop_sequence'])

        settlement_filter = self.settlements_filter_df
        settlement_filter = settlement_filter.loc[settlement_filter['Corridor'] == corridor_id]

        filter_indexes = []
        for settlement in settlement_filter['settlement'].values:
            if corridor_timetable_merged['settlement'][corridor_timetable_merged['settlement'] == settlement].index.tolist():
                filter_indexes.append(corridor_timetable_merged['settlement'][corridor_timetable_merged['settlement'] == settlement].index.tolist())

        filter_indexes = list(chain.from_iterable(filter_indexes))
        filter_indexes = [i for i in corridor_timetable_merged.index.values if i not in filter_indexes]

        corridor_timetable_merged = corridor_timetable_merged.drop(labels=filter_indexes).fillna(0)

        del_cols = []
        for series_name, series in corridor_timetable_merged.items():

            # Count the number of non-zero values in each column
            non_zero_count = len([i for i in series.values if i != 0])
            # If there are fewer than 2 non-zero values, mark the column for deletion
            if non_zero_count < 2:
                del_cols.append(series_name)

        # Drop the marked columns outside the loop
        corridor_timetable_merged = corridor_timetable_merged.drop(columns=del_cols)

        # Save the output
        output_path = os.path.join(self.output_directory, f'corridor_timetable_{corridor_id}.csv')
        corridor_timetable_merged.to_csv(output_path, index=False)
        print(f'Timetable for corridor {corridor_id} saved to {output_path}.')

    def build(self) -> ...:
        corridor_ids = self.routes_corridors_df['corridor_id'].unique()
        for corridor_id in corridor_ids: self._build_timetable_for_corridor(corridor_id) 

class ServiceTypes(Enum):
    NO_SERVICE = 0
    MON_FRI = 1
    MON = 2
    TUE = 3
    WED = 4
    THU = 5
    FRI = 6
    SAT = 7
    SUN = 8 
    MON_THU = 9
    MON_SAT = 10
    MON_SUN = 11
    MON_THU_SAT = 13
    TUE_THU = 14

@dataclass
class Stop:
    stop_id: str
    stop_name: str
    stop_latitude: float
    stop_longitude: float
    settlement: str
    county: str
    service_type: ServiceTypes

    def __str__(self) -> str:
        return f'{self.stop_id}: {self.stop_name.upper()} IN {self.settlement.upper()}'

@dataclass
class StopTime: 
    stop_id: str
    stop_name: str
    arrival_time: float
    departure_time: float
    dwel_time: float

    def __str__(self) -> str:
        return f'ARRIVAL TIME: {time.strftime("%H:%M:%S", self.arrival_time)}\nDEPARTURE TIME: {time.strftime("%H:%M:%S", self.departure_time)}'

@dataclass
class Trip:
    trip_id: str
    stops: list[Stop]
    stop_times: list[StopTime]
    stop_sequence: dict[Stop: int]

@dataclass
class Route:
    route_id: str
    agency_id: str
    route_short_name: str
    route_long_name: str
    route_type: int
    trips: list[Trip]

class CorridorLoadMethod(Enum):
    from_csv = 1

@dataclass
class Corridor:
    corridor_id: int
    corridor_name: str
    stops: list[Stop] = field(default_factory=list)
    routes: list[Route] = field(default_factory=list)

    def __contains__(self, service) -> bool:
        return service in self.services

    def load_timetable(self, corridor_timetable_path: str) -> ...:
        self.timetable = pd.read_csv(corridor_timetable_path)
        return self.timetable

    def build_from_csv(self, corridor_services_csv_path: str) -> ...:
        services = []
        with open(corridor_services_csv_path) as csvfile:
            reader = csv.DictReader(csvfile)
            for row in reader:
                if row['corridor_id'] == self.corridor_id:
                    services.append([row['route_id'], row['route_short_name']])
        
    def _load_gtfs_stops_csv(self, stops_file_path: str) -> list[Stop]:
        with open(stops_file_path) as csvfile:
            reader = csv.DictReader(csvfile)
            for row in reader:
                self.stops.append(Stop(
                    stop_id=row['stop_id'],
                    stop_name=row['stop_name'],
                    stop_latitude=row['stop_lat'],
                    stop_longitude=row['stop_lon'],
                    settlement=row['settlement'],
                    county=row['county'],
                    service_type=None
                ))
        return self.stops
    
    def _load_services(self, services_file_path: str, corridor_services: list = None) -> list[Service]:
        with open(services_file_path) as csvfile:
            reader = csv.DictReader(csvfile)
            for row in reader:
                if corridor_services and row['route_id'] in corridor_services:
                    self.services.append(Service(
                        route_id=row['route_id'],
                        agency_id=row['agency_id'],
                        route_short_name=row['route_short_name'],
                        route_long_name=row['route_long_name'],
                        route_type=row['route_type'],
                        stops=None,
                        stop_sequence=None,
                        stop_times=None,
                        service_type=None
                    ))
                else: raise ValueError(f'No corridor_services specified, or services not in corridor')
        return

    def _

    def _match_stops(self, corridor_services: list) -> ...:
        for service in self.services: 
            return

    def _get_frequency(self, start_time: str, end_time: str, day_of_week: DayOfWeek) -> list:
        return

    def _build_timetable(self) -> ...:
        return

    def to_csv(self, path: str) -> None:
        pass

if __name__ == "__main__":
    nta = NTATimeTable(
        stop_times_file_path=r"\\londonfile\ProjectData\IE01T23B25_LOT4_NTA_Secondment\data\GTFS NTA\GTFS NTA\stop_times.csv",
        trips_file_path=r"\\londonfile\ProjectData\IE01T23B25_LOT4_NTA_Secondment\data\GTFS NTA\GTFS NTA\trips.csv",
        routes_file_path=r"\\londonfile\ProjectData\IE01T23B25_LOT4_NTA_Secondment\data\GTFS NTA\GTFS NTA\routes.csv",
        stops_file_path=r"\\londonfile\ProjectData\IE01T23B25_LOT4_NTA_Secondment\data\GTFS NTA\GTFS NTA\stops_filter.csv",
        calendar_file_path=r"\\londonfile\ProjectData\IE01T23B25_LOT4_NTA_Secondment\data\GTFS NTA\GTFS NTA\calendar.csv",
        routes_corridors_file_path=r"\\londonfile\ProjectData\IE01T23B25_LOT4_NTA_Secondment\data\GTFS NTA\GTFS NTA\routes_corridors.csv",
        settlements_filter_file_path=r"\\londonfile\ProjectData\IE01T23B25_LOT4_NTA_Secondment\data\GTFS NTA\GTFS NTA\settlement_filter.csv",
        output_directory=os.path.join(os.getcwd(), 'output')
    )
    nta.build()