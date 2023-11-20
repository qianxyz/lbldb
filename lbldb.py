import csv
import re
from typing import Callable, Optional, Sequence


class Filter:
    def __init__(self, f: Callable[[dict], bool]) -> None:
        self.f = f

    def __call__(self, row: dict) -> bool:
        return self.f(row)

    def __and__(self, other: "Filter") -> "Filter":
        return Filter(lambda r: self(r) and other(r))

    def __or__(self, other: "Filter") -> "Filter":
        return Filter(lambda r: self(r) or other(r))

    def __invert__(self) -> "Filter":
        return Filter(lambda r: not self(r))


class Column:
    def __init__(self, dbid: int, name: str) -> None:
        self.dbid = dbid
        self.name = name

    def __eq__(self, other) -> Filter:
        return Filter(lambda r: type(other)(r[self.name]) == other)

    def __ne__(self, other) -> Filter:
        return Filter(lambda r: type(other)(r[self.name]) != other)

    def __lt__(self, other) -> Filter:
        return Filter(lambda r: type(other)(r[self.name]) < other)

    def __le__(self, other) -> Filter:
        return Filter(lambda r: type(other)(r[self.name]) <= other)

    def __gt__(self, other) -> Filter:
        return Filter(lambda r: type(other)(r[self.name]) > other)

    def __ge__(self, other) -> Filter:
        return Filter(lambda r: type(other)(r[self.name]) >= other)

    def isin(self, seq: Sequence[str]) -> Filter:
        return Filter(lambda r: r[self.name] in seq)

    def matches(self, regex: str) -> Filter:
        return Filter(lambda r: re.match(regex, r[self.name]) is not None)


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

        # register `Column` objects
        for name in self.fieldnames:
            setattr(self, name, Column(id(self), name))

    def __iter__(self) -> csv.DictReader:
        self.stream.seek(0)
        return csv.DictReader(self.stream)

    def append(self, record: dict) -> None:
        self.stream.seek(0, 2)  # to the end
        writer = csv.DictWriter(self.stream, fieldnames=self.fieldnames)
        writer.writerow(record)


class Query:
    def __init__(self, db: Database) -> None:
        self.db = db
        self.projections: list[Column] = []
        self.filters: list[Filter] = []

    def project(self, *args: Column) -> "Query":
        self.projections.extend(args)
        return self

    def filter(self, *args: Filter) -> "Query":
        self.filters.extend(args)
        return self

    def execute(self) -> None:
        for row in self.db:
            # apply filtering
            if not all(f(row) for f in self.filters):
                continue
            # apply projection (if any)
            if len(self.projections) != 0:
                row = {k.name: row[k.name] for k in self.projections}
            print(row)
