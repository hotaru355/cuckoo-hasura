from __future__ import annotations
from argparse import Namespace
from ast import (
    alias,
    AnnAssign,
    Assign,
    AST,
    Attribute,
    Call,
    ClassDef,
    Constant,
    Expr,
    fix_missing_locations,
    If,
    ImportFrom,
    keyword,
    Load,
    Module,
    Name,
    Pass,
    Store,
    Subscript,
    Tuple,
    unparse,
)
from logging import getLogger
from pathlib import Path
from typing import Callable, Optional, Union

from graphql import (
    GraphQLField,
    GraphQLList,
    GraphQLNonNull,
    GraphQLObjectType,
    GraphQLScalarType,
)


class PackageInitModule:
    """
    The top level `__init__.py` Python module.

    - Exports all schema folders.
    - Uses late binding to define model relations.
        See https://docs.pydantic.dev/dev-v1/usage/postponed_annotations/
    """

    def __init__(
        self,
        cli_args: Namespace,
    ):
        self._cli_args = cli_args
        self._file = Path(f"{cli_args.output_dir}/__init__.py")
        if not cli_args.no_wipe and self._file.exists():
            self._file.unlink()

        self.imports: set[ImportFrom] = set()
        self._update_forward_refs: set[Expr] = set()

    def write_to_file(self):
        module = Module(
            body=[
                *sorted(self.imports, key=lambda imp: imp.module),
                *sorted(
                    self._update_forward_refs,
                    key=lambda ref: f"{ref.value.func.value.value.id}.{ref.value.func.value.attr}",
                ),
            ],
            type_ignores=[],
        )
        fix_missing_locations(module)

        with self._file.open("w") as f:
            f.write(unparse(module) + "\n")

    def update_forward_ref(
        self,
        schema_name: str,
        model_class_name: str,
        relation_schema_name: str,
        relation_class_name: str,
    ):
        try:
            expr = next(
                filter(
                    lambda expr: expr.value.func.value.attr == model_class_name,
                    self._update_forward_refs,
                )
            )
        except StopIteration:
            expr = Expr(
                value=Call(
                    func=Attribute(
                        value=Attribute(
                            value=Name(id=schema_name, ctx=Load()),
                            attr=model_class_name,
                            ctx=Load(),
                        ),
                        attr="update_forward_refs",
                        ctx=Load(),
                    ),
                    args=[],
                    keywords=[],
                )
            )
            self._update_forward_refs.add(expr)

        if not list(
            filter(
                lambda keyword: keyword.arg == relation_class_name, expr.value.keywords
            )
        ):
            expr.value.keywords.append(
                keyword(
                    arg=relation_class_name,
                    value=Attribute(
                        value=Name(id=relation_schema_name, ctx=Load()),
                        attr=relation_class_name,
                        ctx=Load(),
                    ),
                ),
            )


class ModelModule:
    class Imports:
        def __init__(self) -> None:
            self.buildin: set[ImportFrom] = set()
            self.external: set[ImportFrom] = set()
            self.relations: Optional[If] = None
            self.internal: set[ImportFrom] = set()

    def __init__(
        self,
        cli_args: Namespace,
        model_type_node: GraphQLObjectType,
        relation_names: set[str],
        module_init_file: Path,
        package_init: PackageInitModule,
    ) -> None:
        self._cli_args = cli_args
        self._model_type_node = model_type_node
        self._relation_names = relation_names
        self._module_init_file = module_init_file
        self._package_init = package_init
        self._imports = ModelModule.Imports()

        # Names:
        (
            self._schema_name,
            self._table_name,
        ) = cli_args.config_object.get_schema_and_table(model_type_node.name)
        (
            self._model_class_name,
            self._base_class_name,
            self._numeric_class_name,
            self._aggregates_name,
        ) = self._cli_args.config_object.get_class_names(self._table_name)

        # Classes:
        self._base_class_def = self._create_class(name=self._base_class_name)
        self._model_class_def = self._create_model_class()
        self._numeric_class_def, aggr_assignment = self._create_aggr_class()

        self._process_fields()

        self._body = [
            *self._imports.buildin,
            *self._imports.external,
            *self._imports.internal,
            *([self._imports.relations] if self._imports.relations is not None else []),
            self._base_class_def,
            self._model_class_def,
            self._numeric_class_def,
            aggr_assignment,
        ]

        self._file_name = self._cli_args.config_object.get_file_name(self)
        self._append_to_module_init()

    def write_to_file(self):
        module = Module(
            body=self._body,
            type_ignores=[],
        )
        fix_missing_locations(module)

        file_path = Path(
            f"{self._cli_args.output_dir}/{self._schema_name}/{self._file_name}.py"
        )
        with file_path.open("w") as f:
            f.write(unparse(module))

    def _append_to_module_init(self):
        import_node = ImportFrom(
            module=f".{self._file_name}",
            names=[
                alias(name=self._model_class_name),
                alias(name=self._aggregates_name),
            ],
            level=0,
        )
        fix_missing_locations(import_node)
        with self._module_init_file.open("a") as f:
            f.write(unparse(import_node) + "\n")

    def _create_aggr_class(self):
        self._add_import(self._imports.external, module="pydantic", name="BaseModel")
        self._add_import(
            self._imports.external,
            module="cuckoo.models",
            name="AggregateResponse",
        )

        return (
            self._create_class(
                name=self._numeric_class_name,
                bases=[Name(id="BaseModel", ctx=Load())],
            ),
            Assign(
                targets=[Name(id=self._aggregates_name, ctx=Store())],
                value=Subscript(
                    value=Name(id="AggregateResponse", ctx=Load()),
                    slice=Tuple(
                        elts=[
                            Name(id=self._base_class_name, ctx=Load()),
                            Name(id=self._numeric_class_name, ctx=Load()),
                            Name(id=self._model_class_name, ctx=Load()),
                        ],
                        ctx=Load(),
                    ),
                    ctx=Load(),
                ),
            ),
        )

    def _create_model_class(self):
        self._add_import(
            self._imports.external,
            module="cuckoo.models",
            name="HasuraTableModel",
        )
        return self._create_class(
            name=self._model_class_name,
            bases=[
                Name(id="HasuraTableModel", ctx=Load()),
                Name(id=self._base_class_name, ctx=Load()),
            ],
            body=[
                Assign(
                    targets=[Name(id="_table_name", ctx=Store())],
                    value=Constant(value=self._table_name),
                ),
            ],
        )

    @staticmethod
    def _add_import(imports: set[ImportFrom], module: str, name: str):
        try:
            module_import = next(filter(lambda imp: imp.module == module, imports))
        except StopIteration:
            module_import = ImportFrom(module=module, names=[], level=0)
            imports.add(module_import)

        if not list(filter(lambda alias: alias.name == name, module_import.names)):
            module_import.names.append(alias(name=name))

        return

    def _add_relation_import(self, module: str, name: str):
        if self._imports.relations is None:
            self._add_import(
                self._imports.buildin, module="typing", name="TYPE_CHECKING"
            )
            self._imports.relations = If(
                test=Name(id="TYPE_CHECKING", ctx=Load()),
                body=[],
                orelse=[],
            )

        create_forward_ref = False
        try:
            module_import = next(
                filter(lambda imp: imp.module == module, self._imports.relations.body)
            )
        except StopIteration:
            module_import = ImportFrom(module=module, names=[], level=0)
            self._imports.relations.body.append(module_import)
            self._add_import(self._imports.buildin, module="typing", name="ForwardRef")
            create_forward_ref = True

        if not list(filter(lambda alias: alias.name == name, module_import.names)):
            module_import.names.append(alias(name=name))
            create_forward_ref = True

        if create_forward_ref:
            self._imports.relations.orelse.append(
                Assign(
                    targets=[Name(id=name, ctx=Store())],
                    value=Call(
                        func=Name(id="ForwardRef", ctx=Load()),
                        args=[Constant(value=name)],
                        keywords=[],
                    ),
                )
            )

        return

    def _get_named_type_node(
        self,
        field: Union[GraphQLList, GraphQLNonNull, GraphQLScalarType],
        _is_list=False,
    ):
        if isinstance(field, GraphQLList):
            return self._get_named_type_node(field.of_type, True)
        elif isinstance(field, GraphQLNonNull):
            return self._get_named_type_node(field.of_type, _is_list)
        else:
            return field, _is_list

    def _process_fields(self):
        fields_by_name = self._init_base_class()

        schema_name, table_name = self._cli_args.config_object.get_schema_and_table(
            self._model_type_node.name
        )
        for field_name, field_node in fields_by_name.items():
            type_node, field_is_list = self._get_named_type_node(field_node.type)
            aggregate_relation_names = {
                f"{name}_aggregate" for name in self._relation_names
            }
            field_type_name = type_node.name
            if (field_type_name in self._relation_names) or (
                field_type_name in aggregate_relation_names
            ):
                self._process_relation(
                    schema_name=schema_name,
                    table_name=table_name,
                    field_name=field_name,
                    field_type_name=field_type_name,
                    field_is_list=field_is_list,
                )
            else:
                self._process_field_mapping(
                    field_name=field_name,
                    field_type_name=field_type_name,
                    field_is_list=field_is_list,
                    field=field_node,
                )

        if not self._numeric_class_def.body:
            # model has no numeric fields
            self._numeric_class_def.body = [Pass()]

    def _process_relation(
        self,
        schema_name: str,
        table_name: str,
        field_name: str,
        field_type_name: str,
        field_is_list: bool,
    ):
        (
            relation_schema_name,
            relation_table_name,
        ) = self._cli_args.config_object.get_schema_and_table(field_type_name)
        self._add_import(self._package_init.imports, module=".", name=self._schema_name)

        if schema_name == relation_schema_name and table_name == relation_table_name:
            # self reference
            self._add_import(
                self._imports.buildin, module="__future__", name="annotations"
            )
        else:
            # reference other model
            if relation_table_name.endswith("_aggregate"):
                (
                    *_,
                    relation_class_name,
                ) = self._cli_args.config_object.get_class_names(
                    relation_table_name.rstrip("_aggregate")
                )
            else:
                (
                    relation_class_name,
                    *_,
                ) = self._cli_args.config_object.get_class_names(relation_table_name)
            self._package_init.update_forward_ref(
                schema_name=schema_name,
                model_class_name=self._model_class_name,
                relation_schema_name=relation_schema_name,
                relation_class_name=relation_class_name,
            )
            relative_path = (
                "."
                if schema_name == relation_schema_name
                else f"..{relation_schema_name}"
            )
            self._add_relation_import(
                module=f"{relative_path}", name=relation_class_name
            )
            self._add_import(self._imports.buildin, module="typing", name="Optional")
            self._model_class_def.body.append(
                AnnAssign(
                    target=Name(id=field_name, ctx=Store()),
                    annotation=Subscript(
                        value=Name(id="Optional", ctx=Load()),
                        slice=(
                            Subscript(
                                value=Name(id="list", ctx=Load()),
                                slice=Name(id=relation_class_name, ctx=Load()),
                                ctx=Load(),
                            )
                            if field_is_list
                            else Name(id=relation_class_name, ctx=Load())
                        ),
                        ctx=Load(),
                    ),
                    simple=1,
                ),
            )

    def _process_field_mapping(
        self,
        field_name: str,
        field_type_name: str,
        field_is_list: bool,
        field: GraphQLField,
    ):
        config = self._cli_args.config_object

        py_type_or_fn = config.MAPPING.get(field_type_name)
        if isinstance(py_type_or_fn, Callable):
            py_type = py_type_or_fn(self, (field_name, field))
        elif isinstance(py_type_or_fn, str):
            py_type = py_type_or_fn
        else:
            py_type = f'"FIXME - GraphQL type: `{field_type_name}`"'
            getLogger("CuckooCodeGen").warning(
                f"GraphQL type `{field_type_name}` in file {self._schema_name}."
                f"{self._table_name}.py does not appear to be a relation to "
                "another GraphQL type nor is it mapped to a python type."
            )
        self._add_import(self._imports.buildin, module="typing", name="Optional")
        self._base_class_def.body.append(
            AnnAssign(
                target=Name(id=field_name, ctx=Store()),
                annotation=Subscript(
                    value=Name(id="Optional", ctx=Load()),
                    slice=(
                        Subscript(
                            value=Name(id="list", ctx=Load()),
                            slice=Name(id=py_type, ctx=Load()),
                            ctx=Load(),
                        )
                        if field_is_list
                        else Name(id=py_type, ctx=Load())
                    ),
                    ctx=Load(),
                ),
                simple=1,
            ),
        )
        if config.is_numeric(self, (field_name, field_type_name, field)):
            self._numeric_class_def.body.append(
                AnnAssign(
                    target=Name(id=field_name, ctx=Store()),
                    annotation=Subscript(
                        value=Name(id="Optional", ctx=Load()),
                        slice=Name(id="float", ctx=Load()),
                        ctx=Load(),
                    ),
                    simple=1,
                )
            )

    def _init_base_class(self):
        fields_by_name: dict[str, GraphQLField] = {
            field_name: field
            for field_name, field in self._model_type_node.fields.items()
        }
        field_names = set(fields_by_name.keys())

        has_common_fields = False
        for common_field_names, base_model_name in [
            ({"uuid"}, "IdentityModel"),
            ({"created_at", "created_by"}, "CreatableModel"),
            ({"updated_at", "updated_by"}, "UpdatableModel"),
            ({"deleted_at", "deleted_by"}, "DeletableModel"),
        ]:
            if common_field_names.issubset(field_names):
                has_common_fields = True
                self._add_import(
                    self._imports.external,
                    module="cuckoo.models",
                    name=base_model_name,
                )
                self._base_class_def.bases.append(Name(id=base_model_name, ctx=Load()))
                for common_field_name in common_field_names:
                    fields_by_name.pop(common_field_name)

        if not has_common_fields:
            self._add_import(
                self._imports.external,
                module="pydantic",
                name="BaseModel",
            )
            self._base_class_def.bases.append(Name(id="BaseModel", ctx=Load()))

        return fields_by_name

    def _create_class(
        self,
        name: str,
        bases: Optional[list[AST]] = None,
        body: Optional[list[AST]] = None,
    ):
        return ClassDef(
            name=name,
            bases=bases or [],
            body=body or [],
            keywords=[],
            decorator_list=[],
        )
