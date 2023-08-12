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
