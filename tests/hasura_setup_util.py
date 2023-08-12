from pathlib import Path
from typing import Literal, Union
from urllib.parse import urljoin
from httpx import post

from cuckoo.constants import HASURA_HEADERS, HASURA_URL


HASURA_METADATA_URL = urljoin(HASURA_URL, "metadata")


def run_sql(sql: str):
    response = post(
        headers=HASURA_HEADERS,
        url=urljoin(HASURA_URL, "query"),
        json={
            "type": "run_sql",
            "args": {"sql": sql},
        },
    )
    response.raise_for_status()
    return response.json()


def run_sql_file(file: Path):
    with file.open() as fp:
        sql = fp.read()

    run_sql(sql)


def clear_metadata():
    response = post(
        headers=HASURA_HEADERS,
        url=HASURA_METADATA_URL,
        json={"type": "clear_metadata", "args": {}},
    )
    response.raise_for_status()


def untrack_tables(table_names: list[str]):
    for full_table_name in table_names:
        schema, table_name = _split_schema_and_table(full_table_name)
        response = post(
            headers=HASURA_HEADERS,
            url=HASURA_METADATA_URL,
            json={
                "type": "pg_untrack_table",
                "args": {
                    "table": {
                        "schema": schema,
                        "name": table_name,
                    },
                    "cascade": True,
                },
            },
        )

        response.raise_for_status()


def track_tables(table_names: list[str]):
    for full_table_name in table_names:
        schema, table_name = _split_schema_and_table(full_table_name)
        response = post(
            headers=HASURA_HEADERS,
            url=HASURA_METADATA_URL,
            json={
                "type": "pg_track_table",
                "args": {
                    "source": "default",
                    "table": {"name": table_name, "schema": schema},
                },
            },
        )

        response.raise_for_status()
    print("Track tables: ", table_names)


def track_functions(
    func_names: list[str],
    exposed_as: Union[Literal["query"], Literal["mutation"]] = "query",
):
    for full_func_name in func_names:
        schema, func_name = _split_schema_and_table(full_func_name)
        response = post(
            headers=HASURA_HEADERS,
            url=HASURA_METADATA_URL,
            json={
                "type": "pg_track_function",
                "args": {
                    "source": "default",
                    "function": {
                        "schema": schema,
                        "name": func_name,
                    },
                    "configuration": {"exposed_as": exposed_as},
                },
            },
        )

        response.raise_for_status()
    print("Track functions: ", func_names)


def create_one_relation(
    src_table_column: str,
    dst_table_column: str,
    relation_name: str,
    fk_constraint_on_src=True,
):
    src_schema, src_table, src_column = _split_schema_and_table_and_column(
        src_table_column
    )
    dst_schema, dst_table, dst_column = _split_schema_and_table_and_column(
        dst_table_column
    )
    fk_constraint = (
        [src_column]
        if fk_constraint_on_src
        else {
            "table": {
                "schema": dst_schema,
                "name": dst_table,
            },
            "columns": [dst_column],
        }
    )
    response = post(
        headers=HASURA_HEADERS,
        url=HASURA_METADATA_URL,
        json={
            "type": "pg_create_object_relationship",
            "args": {
                "table": {
                    "schema": src_schema,
                    "name": src_table,
                },
                "name": relation_name,
                "using": {"foreign_key_constraint_on": fk_constraint},
            },
        },
    )

    response.raise_for_status()


def create_many_relation(
    src_table_name: str,
    dst_table_column: str,
    relation_name: str,
):
    src_schema, src_table = _split_schema_and_table(src_table_name)
    dst_schema, dst_table, dst_column = _split_schema_and_table_and_column(
        dst_table_column
    )
    response = post(
        headers=HASURA_HEADERS,
        url=HASURA_METADATA_URL,
        json={
            "type": "pg_create_array_relationship",
            "args": {
                "table": {
                    "schema": src_schema,
                    "name": src_table,
                },
                "name": relation_name,
                "using": {
                    "foreign_key_constraint_on": {
                        "table": {
                            "schema": dst_schema,
                            "name": dst_table,
                        },
                        "columns": [dst_column],
                    }
                },
            },
        },
    )

    response.raise_for_status()


def _split_schema_and_table(full_table_name: str):
    if full_table_name.count(".") != 1:
        raise ValueError(
            f"Table name argument '{full_table_name}' needs to be in the format of "
            "<schema>.<table>."
        )
    return full_table_name.split(".")


def _split_schema_and_table_and_column(full_table_column: str):
    if full_table_column.count(".") != 2:
        raise ValueError(
            f"Column name argument '{full_table_column}' needs to be in the format of "
            "<schema>.<table>.<column>"
        )
    return full_table_column.split(".")
