from functools import reduce
from inspect import isclass
from typing import (
    Any,
<<<<<<<< HEAD:cuckoo/models/vendors.py
    Union,
    get_args,
    get_origin,
)

from pydantic.fields import ModelField

from .buildins import AggregateResponse, TableModel


class PydanticV1TableModel(TableModel):
    @classmethod
========
    ClassVar,
    Generator,
    Generic,
    Optional,
    TypeVar,
)

from pydantic import BaseModel, Extra
from pydantic.generics import GenericModel


class TableModel(BaseModel, ABC):
    _table_name: ClassVar[str]
    """The name of the table in the database."""

    @classmethod
    @abstractmethod
>>>>>>>> 78bf0a3d725aaddb1261c356ef51ae4910bdd18b:cuckoo/models/builtins.py
    def fields(
        cls,
        include_inherited=True,
        include_relations=False,
<<<<<<<< HEAD:cuckoo/models/vendors.py
    ):
        all_class_fields = set(cls.__fields__.items())
        if include_inherited and hasattr(super(), "__fields__"):
            all_class_fields.update((super().__fields__.items()))
        for field_name, field_model in all_class_fields:
            if field_name == "_table_name":
                continue
            field_type = field_model.type_
            if (not include_relations) and (cls._is_model_type(field_type)):
                continue
            yield (
                field_name,
                field_type,
                (cls._is_list_field(field_model)),
            )

    def to_hasura_input(self):
        return reduce(self._convert_sub_models, self.fields(include_relations=True), {})

    @classmethod
    def _is_list_field(cls, value: ModelField):
        return get_origin(value.annotation) is list or cls._is_union_of_list_field(
            value
        )

    @classmethod
    def _is_model_type(cls, value: Any):
        return isclass(value) and issubclass(value, (TableModel, AggregateResponse))

    @classmethod
    def _is_union_of_list_field(cls, value: ModelField):
        return get_origin(value.annotation) is Union and any(
            [get_origin(sub) is list for sub in get_args(value.annotation)]
        )

    def _convert_sub_models(
========
    ) -> list[tuple[str, Any, bool]]:
        """List all fields of the table model with name, type, and metadata.

        Args:
            include_inherited (bool, optional):
                Include fields from all super classes that also derive from `TableModel`
                (such as models from `models.common`, for example). Defaults to True.
            include_relations (bool, optional):
                Include fields that relate to other `TableModel`s. Defaults to False.

        Returns:
            list[tuple[str, Any, bool]]: a tuple (field_name, field_type, is_multi)
        """
        ...

    def to_hasura_input(self) -> dict[str, Any]:
        return reduce(
            self._sub_model_to_hasura_input,
            self.fields(include_relations=True),
            {},
        )

    @classmethod
    def _is_table_model_class(cls, value: Any):
        return isclass(value) and issubclass(value, (TableModel))

    @classmethod
    def _is_aggregate_model_class(cls, value: Any):
        return isclass(value) and issubclass(value, (AggregateResponse))

    @classmethod
    def _get_field_type(cls, field_name: str):
        _, field_type, _ = next(
            filter(
                lambda iter_field_name: iter_field_name[0] == field_name,
                cls.fields(include_relations=True),
            ),
            (None, None, None),
        )
        return field_type

    def _sub_model_to_hasura_input(
>>>>>>>> 78bf0a3d725aaddb1261c356ef51ae4910bdd18b:cuckoo/models/builtins.py
        self,
        data_aggregate: dict[str, Any],
        field_info: tuple[str, Any, bool],
    ):
        field_name, field_type, is_multi = field_info
        field_value = getattr(self, field_name)
        if field_value is None:
            return data_aggregate
<<<<<<<< HEAD:cuckoo/models/vendors.py

        if self._is_model_type(field_type):
            data_aggregate.update(
                {
                    field_name: {
                        "data": [model.to_hasura_input() for model in field_value]
                        if is_multi
                        else field_value.to_hasura_input()
========
        if self._is_aggregate_model_class(field_type):
            return data_aggregate

        if self._is_table_model_class(field_type):
            data_aggregate.update(
                {
                    field_name: {
                        "data": (
                            field_value.to_hasura_input()
                            if not is_multi
                            else [model.to_hasura_input() for model in field_value]
                        )
>>>>>>>> 78bf0a3d725aaddb1261c356ef51ae4910bdd18b:cuckoo/models/builtins.py
                    }
                }
            )
        else:
            data_aggregate.update({field_name: field_value})

        return data_aggregate
<<<<<<<< HEAD:cuckoo/models/vendors.py
========

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
>>>>>>>> 78bf0a3d725aaddb1261c356ef51ae4910bdd18b:cuckoo/models/builtins.py
