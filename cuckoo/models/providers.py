from typing import (
    Union,
    get_args,
    get_origin,
)

from pydantic.fields import ModelField

from .builtins import TableModel


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
        return [
            (
                field_name,
                field_model.type_,
                cls._is_list_field(field_model),
            )
            for field_name, field_model in all_class_fields
            if (
                include_relations
                or not (
                    cls._is_table_model_class(field_model.type_)
                    or cls._is_aggregate_model_class(field_model.type_)
                )
            )
        ]

    @classmethod
    def _is_list_field(cls, value: ModelField):
        return get_origin(value.annotation) is list or cls._is_union_of_list_field(
            value
        )

    @classmethod
    def _is_union_of_list_field(cls, value: ModelField):
        """True, if the type is `Union[list[Any], Any]`"""
        return get_origin(value.annotation) is Union and any(
            [get_origin(sub) is list for sub in get_args(value.annotation)]
        )
