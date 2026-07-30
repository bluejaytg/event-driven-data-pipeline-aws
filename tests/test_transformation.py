"""Tests for PySpark batch transformation functions."""

from pyspark_jobs.batch_transformation import transform_records


def test_transform_records_uppercases_strings() -> None:
    records = [
        {"id": 1, "name": "alice", "value": "hello"},
        {"id": 2, "name": "bob", "value": "world"},
    ]

    expected = [
        {"id": 1, "name": "ALICE", "value": "HELLO"},
        {"id": 2, "name": "BOB", "value": "WORLD"},
    ]

    assert transform_records(records) == expected
