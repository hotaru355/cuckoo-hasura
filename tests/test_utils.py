from typing import Generator

from pytest import raises, mark
from geojson_pydantic import Point

from cuckoo.utils import grouper, to_truncated_str, to_sql_function_args


class TestToTruncatedStr:
    def test_string_longer_than_limit_gets_truncated(self):
        test_input = "A" * 2000
        expected = "A" * 1000 + "..."

        actual = to_truncated_str(test_input)

        assert actual == expected

    def test_string_shorter_than_limit_does_not_get_truncated(self):
        test_input = "A" * 500
        expected = test_input

        actual = to_truncated_str(test_input)

        assert actual == expected


class TestGrouper:
    def test_group_size_is_smaller_than_iterable_and_evenly_divisible(self):
        test_iterable = [1, 2, 3, 4, 5, 6]
        expected = [(1, 2, 3), (4, 5, 6)]

        actual = grouper(test_iterable, 3)

        assert isinstance(actual, Generator)
        assert list(actual) == expected

    def test_group_size_is_smaller_than_iterable_and_not_evenly_divisible(self):
        test_iterable = [1, 2, 3, 4, 5, 6, 7]
        expected = [(1, 2, 3), (4, 5, 6), (7,)]

        actual = grouper(test_iterable, 3)

        assert isinstance(actual, Generator)
        assert list(actual) == expected

    def test_group_size_is_larger_than_iterable(self):
        test_iterable = [1, 2, 3, 4, 5, 6, 7]
        expected = [(1, 2, 3, 4, 5, 6, 7)]

        actual = grouper(test_iterable, 9)

        assert isinstance(actual, Generator)
        assert list(actual) == expected

    def test_group_size_is_one(self):
        test_iterable = [1, 2, 3, 4, 5, 6, 7]
        expected = [(1,), (2,), (3,), (4,), (5,), (6,), (7,)]

        actual = grouper(test_iterable, 1)

        assert isinstance(actual, Generator)
        assert list(actual) == expected

    def test_invalid_group_size_less_than_one(self):
        test_iterable = [1, 2, 3, 4, 5, 6, 7]

        with raises(ValueError):
            grouper(test_iterable, -1)

    def test_invalid_group_size_not_int(self):
        test_iterable = [1, 2, 3, 4, 5, 6, 7]

        with raises(ValueError):
            grouper(test_iterable, 3.5)  # type: ignore


class TestToSqlFunctionArgs:
    @mark.parametrize(
        argnames=["args", "expected"],
        argvalues=[
            [None, None],
            [{"test": [1, 2, 3]}, {"test": "{1,2,3}"}],
            [{"test": {1, 2, 3}}, {"test": "{1,2,3}"}],
            [{"test": (n for n in [1, 2, 3])}, {"test": "{1,2,3}"}],
            [{"test": True}, {"test": True}],
            [{"test": 1}, {"test": 1}],
            [{"test": 1.1}, {"test": 1.1}],
            [
                {"test": Point(type="Point", coordinates=(1, 1))},
                {"test": '{"type":"Point","coordinates":[1.0,1.0],"bbox":null}'},
            ],
            [{"test": "string"}, {"test": "string"}],
        ],
        ids=[
            "NONE",
            "list",
            "set",
            "generator",
            "bool",
            "int",
            "float",
            "BaseModel",
            "string",
        ],
    )
    def test_function_args_are_returned(self, args, expected):
        actual = to_sql_function_args(args=args)

        assert actual == expected
