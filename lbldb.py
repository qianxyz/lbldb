import csv
from typing import IO, Callable, Optional


class Database:
    def __init__(self, stream: IO, fieldnames: list[str]) -> None:
        self.stream = stream
        self.fieldnames = fieldnames

    @classmethod
    def new(cls, path: str, fieldnames: list[str]) -> "Database":
        stream = open(path, "x", newline="")
        writer = csv.DictWriter(stream, fieldnames=fieldnames)
        writer.writeheader()
        return cls(stream, fieldnames)

    @classmethod
    def load(cls, path: str) -> "Database":
        stream = open(path, "r+", newline="")
        reader = csv.DictReader(stream)
        fieldnames = reader.fieldnames
        assert fieldnames is not None, "csv file has no header"
        return cls(stream, list(fieldnames))

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
