from typing import Literal, Union

from graphql import GraphQLField, GraphQLNamedType
from caseconverter import pascalcase

from codegen.model_module import ModelModule


def with_import(
    module: str,
    name: str,
    import_type: Union[Literal["buildin"], Literal["external"]] = "buildin",
):
    def add_import(
        model_module: ModelModule,
        _: tuple[str, GraphQLField],
    ):
        model_module._add_import(
            getattr(model_module._imports, import_type), module, name
        )
        return name

    return add_import


class DefaultConfig:
    MAPPING = {
        "bigint": "int",
        "Boolean": "bool",
        "date": with_import("datetime", "date"),
        "Float": "float",
        "float8": "float",
        "Int": "int",
        "json": "dict",
        "jsonb": "dict",
        "money": "float",
        "numeric": "float",
        "smallint": "int",
        "String": "str",
        "timestamp": with_import("datetime", "datetime"),
        "timestamptz": with_import("datetime", "datetime"),
        "uuid": with_import("uuid", "UUID"),
    }

    @staticmethod
    def get_schema_and_table(name: str) -> tuple[str, str]:
        schema = "public"
        table = name

        return (schema, table)

    @staticmethod
    def get_class_names(table_name: str) -> tuple[str, str, str, str]:
        model_class_name = pascalcase(table_name)
        base_class_name = f"{model_class_name}Base"
        numeric_class_name = f"{model_class_name}Numerics"
        aggregates_name = f"{model_class_name}Aggregate"

        return model_class_name, base_class_name, numeric_class_name, aggregates_name

    @staticmethod
    def get_file_name(model_module: ModelModule):
        return model_module._table_name

    @staticmethod
    def is_numeric(
        model_module: ModelModule,
        field_info: tuple[str, str, GraphQLField],
    ) -> bool:
        field_name, field_type_name, field = field_info
        return field_type_name in ["Int", "Float", "numeric"]

    @staticmethod
    def filter_tables(
        schema_and_table: tuple[str, str], node: GraphQLNamedType
    ) -> bool:
        return True
