from enum import Enum
from itertools import zip_longest
from types import GeneratorType
from typing import Any, Generator, Iterable, TypeVar, Union

import orjson
from pydantic import BaseModel


class BracketStyle(tuple, Enum):
    ROUND = ("(", ")")
    CURLY = ("{", "}")
    SQUARE = ("[", "]")


def in_brackets(stringable: str, style=BracketStyle.ROUND, condition=True):
    before_bracket, after_bracket = style if condition else ("", "")
    return f"{before_bracket} {stringable} {after_bracket}"


def to_sql_function_args(args: Union[dict, None]):
    if args is None:
        return None

    def to_fn_value(arg_value):
        if isinstance(arg_value, (list, set, tuple, frozenset, GeneratorType)):
            return "{" + ",".join(str(item) for item in list(arg_value)) + "}"
        elif isinstance(arg_value, (bool, int, float)):
            return arg_value
        elif isinstance(arg_value, BaseModel):
            return orjson.dumps(arg_value.dict()).decode("utf-8")
        else:
            return str(arg_value)

    return {arg_name: to_fn_value(arg_value) for arg_name, arg_value in args.items()}


def to_truncated_str(input: Any, limit=1000):
    input_as_str = str(input)
    num_chars = len(input_as_str)
    if num_chars > limit:
        return input_as_str[:limit] + "..."
    else:
        return input_as_str


def to_compact_str(input_string: str):
    return " ".join(input_string.split())


T = TypeVar("T")


def grouper(
    iterable: Iterable[T],
    group_size: int,
) -> Generator[tuple[T, ...], None, None]:
    """
    Collect data into tuples of size n. Note that the last tuple is shorter than n, if
    `len(iterable)` is not divisible by n.

    See: https://docs.python.org/3/library/itertools.html#itertools-recipes
    Also: https://stackoverflow.com/questions/38054593/zip-longest-without-fillvalue

    Example: grouper('ABCDEFG', 3) -> ('A', 'B', 'C'), ('D', 'E', 'F'), ('G')
    """

    if not isinstance(group_size, int) or group_size < 1:
        raise ValueError("The group size needs to be 1 or higher")

    iterables = [iter(iterable)] * group_size
    return (
        tuple(entry for entry in iterable if entry is not None)
        for iterable in zip_longest(*iterables, fillvalue=None)
    )


class Prop:
    """Create dictionaries for `where` and `order_by` clauses.

    Example:
    ```py
    assert Prop.and_(
        Prop("name").like_("sam%"),
        Prop("date").gt_(some_date)
    ) == {
        "_and": [
            "name": {"_like": "sam%" },
            "date": {"_gt": some_date },
        ]
    }
    ```
    """

    def __init__(self, field_name: str) -> None:
        self._field_name = field_name

    @staticmethod
    def merge(*dicts: dict):
        """Combine multiple dictionaries into a single dictionary. Note: duplicate keys
        will be overwritten.

        Returns:
            dict: The merged dictionary.
        """
        return {key: value for one_dict in dicts for key, value in one_dict.items()}

    @staticmethod
    def and_(*props: dict):
        return {"_and": list(props)}

    @staticmethod
    def or_(*props: dict):
        return {"_or": list(props)}

    @staticmethod
    def not_(*props: dict):
        return {
            "_not": Prop.merge(*props),
        }

    def with_(self, *props: dict):
        """Create sub-directories for `where` clauses on relations.

        Example:
        ```py
        assert Prop("articles").with_(
            Prop("title").eq_("cuckoo"),
            Prop("word_count").lt_(100)
        ) == {
            "articles": {
                "title": {"_eq": "cuckoo"},
                "word_count": {"_lt": 100}
            }
        }
        ```
        """

        return {
            self._field_name: Prop.merge(*props),
        }

    def like_(self, value: str):
        return {self._field_name: {"_like": value}}

    def ilike_(self, value: str):
        return {self._field_name: {"_ilike": value}}

    def nilike_(self, value: str):
        return {self._field_name: {"_nilike": value}}

    def similar_(self, value: str):
        return {self._field_name: {"_similar": value}}

    def nsimilar_(self, value: str):
        return {self._field_name: {"_nsimilar": value}}

    def regex_(self, value: str):
        return {self._field_name: {"_regex": value}}

    def iregex_(self, value: str):
        return {self._field_name: {"_iregex": value}}

    def nregex_(self, value: str):
        return {self._field_name: {"_nregex": value}}

    def contains_(self, value: dict):
        return {self._field_name: {"_contains": value}}

    def contained_in_(self, values: list):
        return {self._field_name: {"_contained_in": values}}

    def has_key_(self, value: Any):
        return {self._field_name: {"_has_key": value}}

    def has_keys_any_(self, values: list):
        return {self._field_name: {"_has_keys_any": values}}

    def eq_(self, value: Any):
        return {self._field_name: {"_eq": value}}

    def neq_(self, value: Any):
        return {self._field_name: {"_neq": value}}

    def gt_(self, value: Any):
        return {self._field_name: {"_gt": value}}

    def lt_(self, value: Any):
        return {self._field_name: {"_lt": value}}

    def gte_(self, value: Any):
        return {self._field_name: {"_gte": value}}

    def lte_(self, value: Any):
        return {self._field_name: {"_lte": value}}

    def in_(self, values: list):
        return {self._field_name: {"_in": values}}

    def nin_(self, value: list):
        return {self._field_name: {"_nin": value}}

    def is_null_(self, value: bool):
        return {self._field_name: {"_is_null": value}}

    def asc_(self):
        """Create `order_by` clauses.

        Examples:
        ```py
        assert Prop("age").asc_() == {
            "age: "asc"
        }


        assert {
            **Prop("age").asc_(),
            **Prop("articles_aggregate").with_(
                Prop("count").desc_()
            )
        } == {
            "age: "asc"
            "articles_aggregate": {
                "count": "desc"
            }
        }
        ```
        """

        return {self._field_name: "asc"}

    def desc_(self):
        """Create `order_by` clauses.

        Examples:
        ```py
        assert Prop("age").desc_() == {
            "age: "desc"
        }


        assert {
            **Prop("age").asc_(),
            **Prop("articles_aggregate").with_(
                Prop("count").desc_()
            )
        } == {
            "age: "asc"
            "articles_aggregate": {
                "count": "desc"
            }
        }
        ```
        """

        return {self._field_name: "desc"}
