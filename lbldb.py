import csv
from typing import Callable, Optional


class Database:
    def __init__(self, path: str, fieldnames: Optional[list[str]] = None) -> None:
        if fieldnames is None:
            # `fieldnames` not provided;
            # assert `path` exists (`r+` does not allow creation)
            # and the file has a header
            self.stream = open(path, "r+", newline="")
            reader = csv.DictReader(self.stream)
            _fieldnames = reader.fieldnames
            assert _fieldnames is not None, "csv file has no header"
            self.fieldnames = list(_fieldnames)
        else:
            # `fieldnames` explicitly provided;
            # assert `path` does not exist (with mode `x+`)
            # and write the header to it
            self.stream = open(path, "x+", newline="")
            writer = csv.DictWriter(self.stream, fieldnames=fieldnames)
            writer.writeheader()
            self.fieldnames = fieldnames

    def __iter__(self) -> csv.DictReader:
        self.stream.seek(0)
        return csv.DictReader(self.stream)

    def append(self, record: dict) -> None:
        self.stream.seek(0, 2)  # to the end
        writer = csv.DictWriter(self.stream, fieldnames=self.fieldnames)
        writer.writerow(record)


class Query:
    def __init__(
        self,
        db: Database,
        projections: Optional[list[str]] = None,
        filters: Optional[list[Callable]] = None,
    ) -> None:
        self.db = db
        self.projections = projections if projections is not None else []
        self.filters = filters if filters is not None else []

    def add_projection(self, *args: str) -> None:
        self.projections.extend(args)

    def add_filter(self, *args: Callable) -> None:
        self.filters.extend(args)

    def execute(self) -> None:
        raise NotImplementedError("TODO")
