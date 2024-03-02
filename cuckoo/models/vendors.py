from functools import reduce
from inspect import isclass
from typing import (
    Any,
    Union,
    get_args,
    get_origin,
)

from pydantic.fields import ModelField

from .buildins import AggregateResponse, TableModel


class PydanticV1TableModel(TableModel):
    @classmethod
    def fields(
        cls,
        include_inherited=True,
        include_relations=False,
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
        self,
        data_aggregate: dict[str, Any],
        field_info: tuple[str, Any, bool],
    ):
        field_name, field_type, is_multi = field_info
        field_value = getattr(self, field_name)
        if field_value is None:
            return data_aggregate

        if self._is_model_type(field_type):
            data_aggregate.update(
                {
                    field_name: {
                        "data": [model.to_hasura_input() for model in field_value]
                        if is_multi
                        else field_value.to_hasura_input()
                    }
                }
            )
        else:
            data_aggregate.update({field_name: field_value})

        return data_aggregate
