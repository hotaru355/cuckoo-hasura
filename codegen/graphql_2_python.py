from __future__ import annotations
from argparse import ArgumentParser, Namespace
from importlib.util import module_from_spec, spec_from_file_location
from itertools import groupby
from pathlib import Path
from shutil import rmtree
from typing import Optional

from graphql import (
    build_client_schema,
    get_introspection_query,
    GraphQLSchema,
)
from graphql.utilities import build_schema
import httpx

from codegen.model_module import ModelModule, PackageInitModule


class GraphQL2Python:
    """
    Parse the Hasura GraphQL schema and generate a pydantic model for each GraphQL type
    that represents a table in the underlying database.

    To find the types that represent a table, there are these possible solutions:
    1. Find the returned type for each `*_py_pk` query
    2. Find the input to each `insert_*` or `insert_*_one` mutation

    This class currently uses approach 1.
    """

    def __init__(self, cli_args: Namespace) -> None:
        self._cli_args = cli_args
        self._schema: GraphQLSchema
        self._parse_schema(
            schema_file=cli_args.schema,
            url=cli_args.URL,
            headers=cli_args.headers,
        )

    def _parse_schema(
        self,
        schema_file: Optional[Path] = None,
        url: Optional[str] = None,
        headers: Optional[dict] = None,
    ):
        if schema_file is not None and schema_file.exists():
            with schema_file.open("r") as f:
                self._schema = build_schema(f.read())
        elif url is not None:
            response = httpx.post(
                url=url,
                headers=headers,
                json={"query": get_introspection_query()},
            )
            response.raise_for_status()
            response_json: dict = response.json()
            if "errors" in response_json:
                raise ConnectionError(response_json["errors"])
            self._schema = build_client_schema(response.json()["data"])
        else:
            raise ValueError(
                "No source for schema provided. Either schema file or hasura URL "
                "required."
            )

    def _get_model_type_nodes(self):
        pk_return_node_names: set[str] = set(
            name.removesuffix("_by_pk")
            for name, _ in self._schema.query_type.fields.items()
            if name.endswith("_by_pk")
        )
        return set(
            node
            for name, node in self._schema.type_map.items()
            if (name in pk_return_node_names)
            and self._cli_args.config_object.filter_tables(
                self._cli_args.config_object.get_schema_and_table(name), node
            )
        )

    def parse(self):
        all_model_type_nodes = sorted(  # groupby() requires sorted input
            self._get_model_type_nodes(), key=lambda node: node.name
        )
        all_model_names = {node.name for node in all_model_type_nodes}
        package_init = PackageInitModule(self._cli_args)

        for schema_name, model_type_nodes_iter in groupby(
            all_model_type_nodes,
            lambda def_node: self._cli_args.config_object.get_schema_and_table(
                def_node.name
            )[0],
        ):
            module_init_file = Path(
                f"{self._cli_args.output_dir}/{schema_name}/__init__.py"
            )
            if not self._cli_args.no_wipe and module_init_file.parent.exists():
                rmtree(module_init_file.parent)
            module_init_file.parent.mkdir(parents=True)
            module_init_file.touch()

            for model_type_node in model_type_nodes_iter:
                module = ModelModule(
                    cli_args=self._cli_args,
                    model_type_node=model_type_node,
                    relation_names=all_model_names,
                    module_init_file=module_init_file,
                    package_init=package_init,
                )
                module.write_to_file()

        package_init.write_to_file()


def run_cli():
    """
    Run the `GraphQL2Python` parser by reading all configuration options from the
    command line.
    TODO: support pyproject.toml project settings
    """
    arg_parser = ArgumentParser(
        prog="Hasura CodeGen",
        description="Create Pydantic models through GraphQL schema inspection",
    )
    schema_input_group = arg_parser.add_mutually_exclusive_group(required=True)
    schema_input_group.add_argument(
        "-U",
        "--URL",
        type=str,
        help=(
            "The URL to the Hasura instance the inspection query should be sent to."
            "Use `--headers` to add auth headers to the request."
        ),
    )
    schema_input_group.add_argument(
        "-S",
        "--schema",
        type=Path,
        help=(
            "The schema file. See https://hasura.io/docs/latest/schema/"
            "common-patterns/export-graphql-schema/ for getting the file from "
            "Hasura."
        ),
    )
    headers: dict[str, str] = {}

    def read_header(str_input: str):
        key, value = str_input.split(":")
        headers.update({key.strip(): value.strip()})

    arg_parser.add_argument(
        "-H",
        "--headers",
        type=read_header,
        action="append",
        nargs=1,
        help=(
            "Headers to be send to Hasura when making the schema inspection query. "
            "For example: "
            '`codegen -H "X-Hasura-Admin-Secret: adminsecretkey" '
            '-H "X-Hasura-Role: admin"`'
        ),
    )
    arg_parser.add_argument(
        "-o",
        "--output_dir",
        default="./tables",
        help=("The directory to write the files to."),
    )
    arg_parser.add_argument(
        "--no_wipe",
        action="store_true",
        help=("Does not clear the output directories before writing new files"),
    )
    arg_parser.add_argument(
        "-c",
        "--config_file",
        type=str,
        help=(
            "The file path to a config object, for example `-c ./my_conf.py::MyConf`. "
            "Uses `default_config.py::DefaultConfig` from the cuckoo package, if not "
            "provided. Class name defaults to `CodegenConfig`, if omitted."
        ),
    )
    cli_args = arg_parser.parse_args()

    if (config_file := cli_args.config_file) is not None:
        SEPARATOR = "::"
        DEFAULT_CLASS_NAME = "CodegenConfig"
        config_file_path, config_class_name = (
            config_file.split(SEPARATOR)
            if (SEPARATOR in config_file)
            else (config_file, DEFAULT_CLASS_NAME)
        )
        spec = spec_from_file_location("config", config_file_path)
        module = module_from_spec(spec)
        spec.loader.exec_module(module)
        if not hasattr(module, config_class_name):
            raise ValueError(
                (
                    f"Could not find class {config_class_name} in "
                    f"file {config_file_path}."
                )
            )
        Config = getattr(module, config_class_name)
    else:
        from codegen.default_config import DefaultConfig as Config
    cli_args.config_object = Config
    cli_args.headers = headers

    GraphQL2Python(cli_args).parse()
