from typing import Generator

from geojson_pydantic import Point
from pytest import mark, raises

from cuckoo.utils import (
    Prop,
    grouper,
    to_compact_str,
    to_sql_function_args,
    to_truncated_str,
)


class TestToCompactStr:
    def test_removes_extra_whitespaces(self):
        input_str = " \n \t \r ABC DEF \n \t \r GHI \n \t \r "
        expected = "ABC DEF GHI"

        actual = to_compact_str(input_str)

        assert actual == expected

    def test_does_not_remove_any_non_consecutive_whitespaces(self):
        input_str = "ABC DEF\nGHI\tJKL"
        expected = "ABC DEF GHI JKL"

        actual = to_compact_str(input_str)

        assert actual == expected


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


@mark.parametrize(
    argnames=["actual", "expected"],
    argvalues=[
        (
            Prop.merge(
                Prop("a").eq_(1),
                Prop("b").neq_(2),
            ),
            {
                "a": {"_eq": 1},
                "b": {"_neq": 2},
            },
        ),
        (
            Prop.and_(
                Prop("a").eq_(1),
                Prop("b").neq_(2),
            ),
            {
                "_and": [
                    {"a": {"_eq": 1}},
                    {"b": {"_neq": 2}},
                ]
            },
        ),
        (
            Prop.or_(
                Prop("a").eq_(1),
                Prop("b").neq_(2),
            ),
            {
                "_or": [
                    {"a": {"_eq": 1}},
                    {"b": {"_neq": 2}},
                ]
            },
        ),
        (
            Prop.not_(
                Prop("a").eq_(1),
                Prop("b").neq_(2),
            ),
            {
                "_not": {
                    "a": {"_eq": 1},
                    "b": {"_neq": 2},
                }
            },
        ),
        (
            Prop("z").with_(
                Prop("a").eq_(1),
                Prop("b").neq_(2),
            ),
            {
                "z": {
                    "a": {"_eq": 1},
                    "b": {"_neq": 2},
                }
            },
        ),
        (Prop("a").like_("%b%"), {"a": {"_like": "%b%"}}),
        (Prop("a").ilike_("%b%"), {"a": {"_ilike": "%b%"}}),
        (Prop("a").nilike_("%b%"), {"a": {"_nilike": "%b%"}}),
        (Prop("a").similar_("b"), {"a": {"_similar": "b"}}),
        (Prop("a").nsimilar_("b"), {"a": {"_nsimilar": "b"}}),
        (Prop("a").regex_("b"), {"a": {"_regex": "b"}}),
        (Prop("a").iregex_("b"), {"a": {"_iregex": "b"}}),
        (Prop("a").nregex_("b"), {"a": {"_nregex": "b"}}),
        (Prop("a").contains_("b"), {"a": {"_contains": "b"}}),
        (Prop("a").contained_in_(["b", "c"]), {"a": {"_contained_in": ["b", "c"]}}),
        (Prop("a").has_key_("b"), {"a": {"_has_key": "b"}}),
        (Prop("a").has_keys_any_(["b", "c"]), {"a": {"_has_keys_any": ["b", "c"]}}),
        (Prop("a").eq_("b"), {"a": {"_eq": "b"}}),
        (Prop("a").neq_("b"), {"a": {"_neq": "b"}}),
        (Prop("a").gt_("b"), {"a": {"_gt": "b"}}),
        (Prop("a").lt_("b"), {"a": {"_lt": "b"}}),
        (Prop("a").gte_("b"), {"a": {"_gte": "b"}}),
        (Prop("a").lte_("b"), {"a": {"_lte": "b"}}),
        (Prop("a").in_(["b", "c"]), {"a": {"_in": ["b", "c"]}}),
        (Prop("a").nin_(["b", "c"]), {"a": {"_nin": ["b", "c"]}}),
        (Prop("a").is_null_(True), {"a": {"_is_null": True}}),
        (Prop("a").asc_(), {"a": "asc"}),
        (Prop("a").desc_(), {"a": "desc"}),
    ],
    ids=[
        "merge",
        "and_",
        "or_",
        "not_",
        "with_",
        "like_",
        "ilike_",
        "nilike_",
        "similar_",
        "nsimilar_",
        "regex_",
        "iregex_",
        "nregex_",
        "contains_",
        "contained_in_",
        "has_key",
        "has_keys_any_",
        "eq_",
        "neq_",
        "gt_",
        "lt_",
        "gte_",
        "lte_",
        "in_",
        "nin_",
        "is_null_",
        "asc_",
        "desc_",
    ],
)
class TestProp:
    def test_methods_create_dictionary(
        self,
        actual: dict,
        expected: dict,
    ):
        assert actual == expected
