from abc import ABC, abstractmethod
from datetime import datetime
from functools import reduce
from inspect import isclass
from typing import Any, Optional, TypeVar, Union, get_args, get_origin
from uuid import UUID

from pydantic import BaseModel, Extra
from pydantic.fields import ModelField


class HasuraTableModel(BaseModel, ABC):
    @property
    @abstractmethod
    def _table_name(self) -> str:
        pass  # pragma: no cover

    def to_hasura_input(self):
        def convert_sub_models(d: dict[str, Any], dict_items: tuple[str, ModelField]):
            key, value = dict_items
            model_value = getattr(self, key)
            if (
                model_value is not None
                and isclass(value.type_)
                and issubclass(value.type_, HasuraTableModel)
            ):
                if (get_origin(value.annotation) is list) or (  # is list[Any] ..
                    get_origin(value.annotation)
                    is Union  # .. or Union[list[Any] | None]
                    and any(
                        [get_origin(sub) is list for sub in get_args(value.annotation)]
                    )
                ):
                    data = [model.to_hasura_input() for model in model_value]
                else:
                    data = model_value.to_hasura_input()

                d.update({key: {"data": data}})
            return d

        return {
            **self.dict(exclude_unset=True),
            **reduce(convert_sub_models, self.__fields__.items(), {}),
        }

    class Config:
        extra = Extra.allow


class IdentityModel(BaseModel):
    uuid: Optional[UUID]


class CreatableModel(BaseModel):
    created_by: Optional[UUID]
    created_at: Optional[datetime]


class UpdatableModel(BaseModel):
    updated_by: Optional[UUID]
    updated_at: Optional[datetime]


class DeletableModel(BaseModel):
    deleted_by: Optional[UUID]
    deleted_at: Optional[datetime]


class UntypedModel(BaseModel):
    class Config:
        allow_population_by_field_name = True
        extra = Extra.allow


TMODEL = TypeVar("TMODEL", bound=HasuraTableModel)
TBATCH_MODEL = TypeVar("TBATCH_MODEL", bound=HasuraTableModel)
