from datetime import datetime
from typing import Optional
from uuid import UUID

from pydantic import BaseModel, Extra


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
