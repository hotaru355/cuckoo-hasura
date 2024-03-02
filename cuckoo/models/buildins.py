from abc import ABC, abstractmethod
from typing import (
    Any,
    Generator,
    Generic,
    Optional,
    TypeVar,
)

from pydantic import BaseModel, Extra
from pydantic.generics import GenericModel


class TableModel(BaseModel, ABC):
    _table_name: str
    """The name of the table in the database."""

    @classmethod
    @abstractmethod
    def fields(
        cls,
        include_inherited=True,
        include_relations=False,
    ) -> Generator[tuple[str, Any, bool], Any, None]:
        """Yields metadata for all defined properties of the table model class.

        Args:
            include_inherited (bool, optional):
                include inherited fields (from models.common for example).
                Defaults to True.
            include_relations (bool, optional):
                include fields that relate to other models. Defaults to False.

        Yields:
            Generator[tuple[str, Any, bool], Any, None]: a tuple (field_name, field_type, is_multi)
        """
        ...

    @abstractmethod
    def to_hasura_input(self) -> dict[str, Any]:
        ...

    @classmethod
    def type_of(
        cls,
        field_name: str,
        include_inherited=True,
        include_relations=False,
    ):
        _, field_type, _ = next(
            filter(
                lambda iter_field_name: iter_field_name[0] == field_name,
                cls.fields(
                    include_inherited=include_inherited,
                    include_relations=include_relations,
                ),
            ),
            (None, None, None),
        )
        return field_type

    class Config:
        extra = Extra.allow


TMODEL = TypeVar("TMODEL", bound=TableModel)
TBATCH_MODEL = TypeVar("TBATCH_MODEL", bound=TableModel)
TNUM_PROPS = TypeVar("TNUM_PROPS")
TMODEL_BASE = TypeVar("TMODEL_BASE")
TBATCHNUM_PROPS = TypeVar("TBATCHNUM_PROPS")
TBATCHMODEL_BASE = TypeVar("TBATCHMODEL_BASE")


class Aggregate(GenericModel, Generic[TMODEL_BASE, TNUM_PROPS]):
    count: Optional[int]
    avg: Optional[TNUM_PROPS]
    max: Optional[TMODEL_BASE]
    min: Optional[TMODEL_BASE]
    stddev: Optional[TNUM_PROPS]
    stddev_pop: Optional[TNUM_PROPS]
    stddev_samp: Optional[TNUM_PROPS]
    sum: Optional[TNUM_PROPS]
    var_pop: Optional[TNUM_PROPS]
    var_samp: Optional[TNUM_PROPS]
    variance: Optional[TNUM_PROPS]


class AggregateResponse(GenericModel, Generic[TMODEL_BASE, TNUM_PROPS, TMODEL]):
    aggregate: Optional[Aggregate[TMODEL_BASE, TNUM_PROPS]]
    nodes: Optional[list[TMODEL]]
