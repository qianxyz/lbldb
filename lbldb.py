import csv
import heapq
import itertools
import json
import re
import shutil
import sys
import tempfile
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
    def __init__(self, dbid: str, name: str) -> None:
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
            setattr(self, name, Column(str(id(self)), name))

    def __iter__(self) -> csv.DictReader:
        self.stream.seek(0)
        return csv.DictReader(self.stream)

    def append(self, record: dict) -> None:
        self.stream.seek(0, 2)  # to the end
        writer = csv.DictWriter(self.stream, fieldnames=self.fieldnames)
        writer.writerow(record)


class Update:
    def __init__(self, db: Database) -> None:
        self.db = db
        self.filters: list[Filter] = []

    def filter(self, *args: Filter) -> "Update":
        self.filters.extend(args)
        return self

    def set(self, column: Column, value):
        self.set_column = column
        self.set_value = value
        return self

    def execute(self):
        # write the updated version to a tempfile
        tmpf = tempfile.TemporaryFile(mode="w+")
        writer = csv.DictWriter(tmpf, fieldnames=self.db.fieldnames)
        writer.writeheader()
        for row in self.db:
            if all(f({str(id(self.db)): row}) for f in self.filters):
                row[self.set_column.name] = self.set_value
            writer.writerow(row)

        # copy the tempfile back
        tmpf.seek(0)
        self.db.stream.seek(0)
        self.db.stream.truncate(0)
        shutil.copyfileobj(tmpf, self.db.stream)

        tmpf.close()


class Delete:
    def __init__(self, db: Database) -> None:
        self.db = db
        self.filters: list[Filter] = []

    def filter(self, *args: Filter) -> "Delete":
        self.filters.extend(args)
        return self

    def execute(self):
        # write the updated version to a tempfile
        tmpf = tempfile.TemporaryFile(mode="w+")
        writer = csv.DictWriter(tmpf, fieldnames=self.db.fieldnames)
        writer.writeheader()
        for row in self.db:
            if all(f({str(id(self.db)): row}) for f in self.filters):
                continue
            writer.writerow(row)

        # copy the tempfile back
        tmpf.seek(0)
        self.db.stream.seek(0)
        self.db.stream.truncate(0)
        shutil.copyfileobj(tmpf, self.db.stream)

        tmpf.close()


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

    def __iter__(self) -> Iterator[dict[str, dict[str, str]]]:
        def product(iterables):
            if not iterables:
                yield {}
                return
            head, *tail = iterables
            for item in head:
                for rest in product(tail):
                    yield {str(id(head)): item, **rest}

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

    def _flatten(
        self, it: Iterator[dict[str, dict[str, str]]]
    ) -> Iterator[dict[str, str]]:
        aliases = [alias for _, alias in self.projections]
        # assert no duplicate column names
        if len(set(aliases)) != len(aliases):
            raise ValueError(f"duplicate column name: {aliases}")
        for row in it:
            # flatten the nested dict
            result = {}
            for column, alias in self.projections:
                result[alias] = column(row)
            yield result

    def execute(self):
        aliases = [alias for _, alias in self.projections]
        # assert no duplicate column names
        if len(set(aliases)) != len(aliases):
            raise ValueError(f"duplicate column name: {aliases}")
        writer = csv.DictWriter(sys.stdout, fieldnames=aliases)
        writer.writeheader()
        for row in self._flatten(iter(self)):
            writer.writerow(row)

    def groupby(self, *columns: Column) -> "Groupby":
        return Groupby(self, *columns)

    def sort(self, column: Column, key=None, reverse=False, debug=False):
        return Sort(self, column, key, reverse, debug)


class Groupby:
    def __init__(self, query: Query, *columns: Column) -> None:
        self.query = query
        self.columns = columns

    def count(self):
        result = defaultdict(int)
        for row in self.query:
            key = tuple(c(row) for c in self.columns)
            result[key] += 1
        writer = csv.DictWriter(
            sys.stdout, fieldnames=[c.name for c in self.columns] + ["count"]
        )
        writer.writeheader()
        for key, count in result.items():
            d = {}
            for c, k in zip(self.columns, key):
                d[c.name] = k
            d["count"] = count
            writer.writerow(d)


class Sort:
    def __init__(self, query, column, key, reverse, debug) -> None:
        self.query = query
        self.column = column
        self.key = key
        self.reverse = reverse
        self.debug = debug
        self._limit = None

    def limit(self, n: int):
        self._limit = n
        return self

    def execute(self):
        it = iter(self.query)
        sorted_it = external_sort(
            it,
            key=self.column if self.key is None else lambda r: self.key(self.column(r)),
            reverse=self.reverse,
            debug=self.debug,
        )

        aliases = [alias for _, alias in self.query.projections]
        # assert no duplicate column names
        if len(set(aliases)) != len(aliases):
            raise ValueError(f"duplicate column name: {aliases}")
        writer = csv.DictWriter(sys.stdout, fieldnames=aliases)
        writer.writeheader()
        it = self.query._flatten(sorted_it)
        if self._limit is not None:
            it = itertools.islice(it, self._limit)
        for row in it:
            writer.writerow(row)


def external_sort(
    it: Iterator, key: Callable, reverse=False, debug=False, chunk_size=16
) -> Iterator:
    tmpfs = []

    # sort the chunks
    while True:
        chunk = list(itertools.islice(it, chunk_size))
        if not chunk:
            break
        chunk.sort(key=key, reverse=reverse)
        if debug:
            tmpf = tempfile.NamedTemporaryFile(mode="w+", delete=False)
        else:
            tmpf = tempfile.TemporaryFile(mode="w+")
        for item in chunk:
            tmpf.write(json.dumps(item) + "\n")
        tmpf.seek(0)
        tmpfs.append(tmpf)
        if len(chunk) < chunk_size:
            break

    # merge until number of runs < chunk size
    while len(tmpfs) > chunk_size:
        new_tmpfs = []

        for i in range(0, len(tmpfs), chunk_size):
            chunk = tmpfs[i : i + chunk_size]
            sorted_its = [map(json.loads, f) for f in chunk]
            if debug:
                tmpf = tempfile.NamedTemporaryFile(mode="w+", delete=False)
            else:
                tmpf = tempfile.TemporaryFile(mode="w+")
            for item in heapq.merge(*sorted_its, key=key, reverse=reverse):
                tmpf.write(json.dumps(item) + "\n")
            tmpf.seek(0)
            new_tmpfs.append(tmpf)

        tmpfs = new_tmpfs

    # yield
    sorted_its = [map(json.loads, f) for f in tmpfs]
    for item in heapq.merge(*sorted_its, key=key, reverse=reverse):
        yield item

    for f in tmpfs:
        f.close()
