import _context

import os
import csv
import multiprocessing as mp
from dataclasses import dataclass
from enum import Enum
from typing import Optional, Union, overload

class GTFSLoadMethod(Enum):
    from_csv = 1
    from_gtfs = 2
    from_postgres = 3
    from_sqlite = 4
    from_transxchange = 5

class LoadCSVFiles(Enum):
    AGENCY = 1
    CALENDAR = 2
    CALENDAR_DATES = 3
    ROUTES = 4
    STOP_TIMES = 5
    STOPS = 6
    TRIPS = 7

@dataclass
class BaseDataLoader: 
    load_method: GTFSLoadMethod

    @overload
    def load(self, file: LoadCSVFiles) -> dict: ...

class GTFSLoadCSV(BaseDataLoader):
    def __init__(self, agency_path: str, calendar_path: str, calendar_dates_path: str, routes_path: str, stop_times_path: str, stops_path: str, trips_path: str) -> None:
        self.agency_path, self.calendar_path, self.calendar_dates_path, self.routes_path, self.stop_times_path, self.stops_path, self.trips_path = agency_path, calendar_path, calendar_dates_path, routes_path, stop_times_path, stops_path, trips_path
        super().__init__(GTFSLoadMethod.from_csv)
        if not self._validate_paths(): raise FileNotFoundError(f'') 
        self.paths = {file: path for file, path in zip(LoadCSVFiles, self.__dict__.values()) if str(path).endswith('.csv')}
        self.csv_files = {file: [] for file in LoadCSVFiles}
        self._to_memory()

    def __call__(self, file: LoadCSVFiles) -> None: self.load(file)
    
    def _validate_paths(self) -> bool: return all([os.path.exists(path) for path in self.__dict__.values() if str(path).endswith('.csv')])

    def _mp_get_chunks(self, file_path: str, max_cpu: int = 8) -> list:
        cpu_count = min(max_cpu, mp.cpu_count())
        file_size = os.path.getsize(file_path)
        chunk_size = file_size // cpu_count
        start_end = []
        with open(file_path, "r+b") as f:

            def is_new_line(position) -> Optional[Union[bool, str]]:
                if position == 0: return True
                else: 
                    f.seek(position -1)
                    return f.read(1) == b"\n"

            def next_line(position) -> ...:
                f.seek(position)
                f.readline()
                return f.tell()
            
            headers = [i.decode('utf8') for i in f.readline().rstrip(b"\r\n").split(b",")]

            chunk_start = 0
            while chunk_start < file_size:
                chunk_end = min(file_size, chunk_start + chunk_size)
                while not is_new_line(chunk_end): chunk_end -= 1
                if chunk_start == chunk_end: chunk_end = next_line(chunk_end)

                start_end.append((file_path, chunk_start, chunk_end))

                chunk_start = chunk_end
    
        return cpu_count, headers, start_end

    def _mp_file_chunk_read(self, file_path: str, chunk_start: int, chunk_end: int) -> dict:
        out = []
        with open(file_path, "rb") as f:
            f.seek(chunk_start)
            for line in f:
                chunk_start += len(line)
                if chunk_start > chunk_end: break
                out.append(line.split(b","))
        return out

    def _mp_file_process(self, file: LoadCSVFiles, cpu_count: int, headers: list[str], start_end: list) -> None:
        with mp.Pool(cpu_count) as p:
            chunk_results: list[dict] = p.starmap(self._mp_file_chunk_read, start_end)
        out = []
        for chunk_result in chunk_results:
            for dline in chunk_result[1:]:
                try: out.append(dict(zip(headers, [d.decode('utf8') for d in dline])))
                except UnicodeDecodeError as e: print(f'WARNING: decode error in {file} -> {e}')
        self.csv_files[file] = out

    @_context.timing("CSV load")
    def _to_memory(self) -> None:
        for file, path, in self.paths.items():
            if os.path.getsize(path) > 1e8:
                print(f"LOG: Loading {path} with multiprocessing")
                cpu_count, headers, *start_end = self._mp_get_chunks(path)
                self._mp_file_process(file, cpu_count, headers, start_end[0])
            else:
                with open(path, 'r', errors='ignore') as f:
                    reader = csv.DictReader(f)
                    self.csv_files[file] = [row for row in reader]

    def load(self, file: LoadCSVFiles) -> csv.DictReader:
        # TODO: BUILD AN OPTIMISER ALGO FOR THIS -> ITERATING THROUGH THE LIST TAKES TOO LONG -> PERHAPS PASSING A LAMBDA FNC TO LOAD()
        return self.csv_files[file]

if __name__ == "__main__":
    gtfs_loader = GTFSLoadCSV('./data/agency.csv', './data/calendar.csv', './data/calendar_dates.csv', './data/routes.csv', './data/stop_times.csv', './data/stops.csv', './data/trips.csv')
    print(gtfs_loader.csv_files[LoadCSVFiles.STOPS])