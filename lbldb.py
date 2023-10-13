import csv
from typing import IO


class Database:
    def __init__(self, stream: IO, fieldnames: list[str]) -> None:
        self.stream = stream
        self.fieldnames = fieldnames

    @classmethod
    def new(cls, path: str, fieldnames: list[str]) -> "Database":
        stream = open(path, 'x', newline="")
        writer = csv.DictWriter(stream, fieldnames=fieldnames)
        writer.writeheader()
        return cls(stream, fieldnames)

    @classmethod
    def load(cls, path) -> "Database":
        stream = open(path, 'r+', newline="")
        reader = csv.DictReader(stream)
        fieldnames = reader.fieldnames
        assert fieldnames is not None, "csv file has no header"
        return cls(stream, list(fieldnames))

    def __iter__(self) -> csv.DictReader:
        self.stream.seek(0)
        return csv.DictReader(self.stream)

    def append(self, record: dict):
        self.stream.seek(0, 2)  # to the end
        writer = csv.DictWriter(self.stream, fieldnames=self.fieldnames)
        writer.writerow(record)


class Query:
    pass
