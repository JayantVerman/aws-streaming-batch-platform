# Reader for headerless delimited raw source files, driven by config/entities.yaml.
# Type casting happens here so every downstream component (producer, batch
# loader) sees identically-typed records.

from __future__ import annotations

import csv
from datetime import datetime
from pathlib import Path
from typing import Any, Iterator

import yaml

# column type -> python caster
_CASTERS = {
    "string": str,
    "long": int,
    "int": int,
    "double": float,
    "timestamp": None,  # handled specially below
}


class RowParseError(ValueError):
    """A raw line couldn't be parsed/cast; carries the raw line for quarantine."""

    def __init__(self, line_no: int, raw: str, reason: str):
        super().__init__(f"line {line_no}: {reason}")
        self.line_no = line_no
        self.raw = raw
        self.reason = reason


def load_yaml(path: str | Path) -> dict[str, Any]:
    with open(path, "r", encoding="utf-8") as fh:
        return yaml.safe_load(fh)


def load_entities(config_path: str | Path = "config/entities.yaml") -> dict[str, dict]:
    return load_yaml(config_path)["entities"]


def _normalize_timestamp(raw: str) -> str:
    raw = raw.strip()
    for fmt in ("%Y-%m-%d %H:%M:%S.%f", "%Y-%m-%d %H:%M:%S", "%Y-%m-%d"):
        try:
            return datetime.strptime(raw, fmt).strftime("%Y-%m-%dT%H:%M:%S")
        except ValueError:
            continue
    raise ValueError(f"unparseable timestamp {raw!r}")


def _cast(raw: str, type_name: str) -> Any:
    if type_name == "timestamp":
        return _normalize_timestamp(raw)
    caster = _CASTERS.get(type_name)
    if caster is None:
        raise ValueError(f"unknown column type {type_name!r}")
    return caster(raw)


def read_records(
    entity: dict[str, Any],
    base_path: str | Path | None = None,
    limit: int | None = None,
) -> Iterator[tuple[int, dict[str, Any] | None, RowParseError | None]]:
    """Yield (line_no, record, error) — exactly one of record/error is set.

    Errors are yielded, not raised, so a single malformed line never kills a
    run; callers route them to quarantine and keep going.
    """
    columns = entity["columns"]
    path = Path(entity["file"])
    if not path.is_absolute():
        path = Path(base_path) / path if base_path else Path("data/sample_raw/retail_db") / path

    delimiter = entity.get("delimiter", ",")
    names = list(columns.keys())
    types = list(columns.values())

    count = 0
    with open(path, "r", encoding="utf-8", newline="") as fh:
        for line_no, row in enumerate(csv.reader(fh, delimiter=delimiter), start=1):
            if not row or (len(row) == 1 and not row[0].strip()):
                continue  # skip blank lines
            try:
                if len(row) != len(names):
                    raise RowParseError(
                        line_no, ",".join(row),
                        f"expected {len(names)} columns, got {len(row)}",
                    )
                record = {
                    name: _cast(value, type_name)
                    for name, value, type_name in zip(names, row, types)
                }
            except RowParseError as exc:
                yield line_no, None, exc
                continue
            except ValueError as exc:
                yield line_no, None, RowParseError(line_no, ",".join(row), str(exc))
                continue
            yield line_no, record, None
            count += 1
            if limit is not None and count >= limit:
                return

# wip142

/* wip */
