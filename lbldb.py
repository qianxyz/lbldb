import csv
import re
from collections import defaultdict
from typing import Callable, Iterator, Optional, Sequence


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

    def __call__(self, row: dict) -> str:
        return row[self.dbid][self.name]

    def __eq__(self, other) -> Filter:
        if isinstance(other, Column):
            return Filter(lambda r: self(r) == other(r))
        return Filter(lambda r: type(other)(self(r)) == other)

    def __ne__(self, other) -> Filter:
        if isinstance(other, Column):
            return Filter(lambda r: self(r) != other(r))
        return Filter(lambda r: type(other)(self(r)) != other)

    def __lt__(self, other) -> Filter:
        return Filter(lambda r: type(other)(self(r)) < other)

    def __le__(self, other) -> Filter:
        return Filter(lambda r: type(other)(self(r)) <= other)

    def __gt__(self, other) -> Filter:
        return Filter(lambda r: type(other)(self(r)) > other)

    def __ge__(self, other) -> Filter:
        return Filter(lambda r: type(other)(self(r)) >= other)

    def isin(self, seq: Sequence[str]) -> Filter:
        return Filter(lambda r: self(r) in seq)

    def matches(self, regex: str) -> Filter:
        return Filter(lambda r: re.match(regex, self(r)) is not None)


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
    def __init__(self, *dbs: Database) -> None:
        self.dbs = dbs
        self.projections: list[tuple[Column, str]] = [
            (getattr(db, fieldname), fieldname)
            for db in dbs
            for fieldname in db.fieldnames
        ]
        self.filters: list[Filter] = []
        self._limit: Optional[int] = None

    def project(self, *args: Column, **kwargs: Column) -> "Query":
        self.projections = [(arg, arg.name) for arg in args] + [
            (arg, alias) for alias, arg in kwargs.items()
        ]
        return self

    def filter(self, *args: Filter) -> "Query":
        self.filters.extend(args)
        return self

    def limit(self, n: int) -> "Query":
        self._limit = n
        return self

    def __iter__(self) -> Iterator[dict[int, dict[str, str]]]:
        def product(iterables):
            if not iterables:
                yield {}
                return
            head, *tail = iterables
            for item in head:
                for rest in product(tail):
                    yield {id(head): item, **rest}

        count = 0
        for row in product(self.dbs):
            # apply filtering
            if not all(f(row) for f in self.filters):
                continue
            # apply projection
            r = defaultdict(dict)
            for p, _ in self.projections:
                r[p.dbid][p.name] = row[p.dbid][p.name]
            yield dict(r)
            count += 1
            if self._limit is not None and count >= self._limit:
                return

    def flatten(self) -> Iterator[dict[str, str]]:
        aliases = [alias for _, alias in self.projections]
        # assert no duplicate column names
        if len(set(aliases)) != len(aliases):
            raise ValueError(f"duplicate column name: {aliases}")
        for row in self:
            # flatten the nested dict
            result = {}
            for column, alias in self.projections:
                result[alias] = column(row)
            yield result

    def execute(self):
        for row in self.flatten():
            print(row)
