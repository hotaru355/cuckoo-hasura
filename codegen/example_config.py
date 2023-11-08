from __future__ import annotations
from logging import getLogger

from graphql import GraphQLField, GraphQLNamedType
import inflect
from caseconverter import pascalcase

from codegen import ModelModule
from codegen.default_config import DefaultConfig


def map_geometry(
    model_module: ModelModule,
    field_info: tuple[str, GraphQLField],
):
    EXCEPTIONS = {
        "sites.sites.location": "Point",
    }
    field_name, field_obj = field_info

    full_field_name = (
        f"{model_module._schema_name}.{model_module._table_name}.{field_name}"
    )
    geometry = EXCEPTIONS.get(full_field_name, "MultiPolygon")

    model_module._add_import(
        model_module._imports.external, "geojson_pydantic", geometry
    )
    return geometry


class CodegenConfig(DefaultConfig):
    MAPPING = {
        **DefaultConfig.MAPPING,
        "geometry": map_geometry,
    }
    _inflect = inflect.engine()

    @staticmethod
    def get_schema_and_table(name: str) -> tuple[str, str]:
        if name.startswith("scan_data"):
            schema = "scan_data"
            table = name.removeprefix("scan_data_")
        else:
            schema, table = name.split("_", maxsplit=1)

        return (schema, table)

    @staticmethod
    def get_class_names(table_name: str) -> tuple[str, str, str, str]:
        if table_name.endswith("_aggregate"):
            model_class_name = pascalcase(table_name)
        else:
            singular_name = CodegenConfig._inflect.singular_noun(table_name)
            if singular_name is False:
                getLogger("CuckooCodeGen").warning(
                    f"Could not singularize {table_name}"
                )
                model_class_name = pascalcase(table_name)
            else:
                model_class_name = pascalcase(singular_name)

        base_class_name = f"{model_class_name}Base"
        numeric_class_name = f"{model_class_name}Numerics"
        aggregates_name = f"{model_class_name}Aggregate"

        return model_class_name, base_class_name, numeric_class_name, aggregates_name

    @staticmethod
    def get_file_name(model_module: ModelModule):
        table_name = model_module._table_name
        singular_name = CodegenConfig._inflect.singular_noun(table_name)
        return table_name if singular_name is False else singular_name

    @staticmethod
    def filter_tables(
        schema_and_table: tuple[str, str], node: GraphQLNamedType
    ) -> bool:
        schema, table = schema_and_table
        return schema in ["auth", "sites"]
