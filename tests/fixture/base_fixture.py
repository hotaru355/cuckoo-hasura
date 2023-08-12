from datetime import date, datetime
from enum import Enum
from uuid import uuid4

from geojson_pydantic import Polygon

from tests.fixture.common_fixture import ParameterizeArgs


UUID = uuid4()
TODAY = date.today()
NOW = datetime.now()


def as_property(inputs: list, is_hashable=True):
    return inputs[0]


def as_list(inputs: list, is_hashable=True):
    return list(inputs)


def as_tuple(inputs: list, is_hashable=True):
    return tuple(inputs)


def as_set(inputs: list, is_hashable=True):
    if not is_hashable:
        return inputs
    return set(inputs)


def as_frozen_set(inputs: list, is_hashable=True):
    if not is_hashable:
        return inputs
    return frozenset(inputs)


def as_generator(inputs: list, is_hashable=True):
    return (item for item in inputs)


class TestEnum(str, Enum):
    TEST = "test"


VARIABLE_SEQUENCES: ParameterizeArgs = {
    "argnames": ["get_variables_seq", "get_expected_seq"],
    "argvalues": [
        [as_property, as_property],
        [as_list, as_list],
        [as_tuple, as_list],
        [as_set, as_list],
        [as_frozen_set, as_list],
        [as_generator, as_list],
    ],
    "ids": [
        "as property",
        "as list",
        "as tuple",
        "as set",
        "as frozen set",
        "as generator",
    ],
}
VARIABLE_TYPES: ParameterizeArgs = {
    "argnames": ["variable_values", "expected_values", "is_hashable"],
    "argvalues": [
        [["test"], ["test"], True],
        [[123], [123], True],
        [[123.123], [123.123], True],
        [[True], [True], True],
        [[UUID], [str(UUID)], True],
        [
            [TODAY],
            [TODAY.isoformat()],  # isoformat = 'YYYY-MM-DD'
            True,
        ],
        [
            [NOW],
            [NOW.isoformat()],  # isoformat = 'YYYY-MM-DD HH:MM:SS.mmmmmm'
            True,
        ],
        [
            [Polygon(type="Polygon", coordinates=[[(0, 1), (1, 1), (1, 0), (0, 1)]])],
            [
                {
                    "type": "Polygon",
                    "coordinates": [[[0.0, 1.0], [1.0, 1.0], [1.0, 0.0], [0.0, 1.0]]],
                    "bbox": None,
                }
            ],
            False,
        ],
        [[None], [None], True],
        [[TestEnum.TEST], [TestEnum.TEST.value], True],
    ],
    "ids": [
        "string",
        "int",
        "float",
        "bool",
        "UUID",
        "date",
        "datetime",
        "geojson",
        "None",
        "Enum",
    ],
}
