# CuckooCodeGen
A code generator for [pydantic](https://docs.pydantic.dev/dev-v1/) models that creates model classes for all DB tables exposed through a Hasura server.

## 0. Install
Install codegen with its minimal requirements:
```sh
pip install cuckoo-hasura[codegen]
```

## 1. Run the code generator
To create some pydantic models that can, but are not limted to ;), the use with cuckoo, run the `codegen` CLI in your favorite shell. See `codegen --help` for more info.

There are 2 options to run the code generator:

- ask the Hasura GraphQL API for the publicly exposed table schemas. This method requires access to the Hasura API through an admin secret. The Hasura configuration needs to allow introspection queries.
- parse a `schema.graphql` file for the publicly exposed table schemas. Suitable option if the GraphQL schema is easier to share as a file.

### 1.1 Generate code from an introspection query

```sh
codegen -U https://hasura/v1/graphql -H 'X-Hasura-Admin-Secret: <PASSWORD>' -H 'X-Hasura-Role: admin'
```

### 1.2 Generate code by parsing a GraphQL file

```sh
codegen -S schema.graphql
```

### 1.3 Options
There are a few CLI options to configure the generator. However, the fine-tuning of the
generated code is handled through a Pyton configuration class that allows customization
of the generated code.

- `-o` | `--output_dir` : The directory the generated files will be written to.
    Defaults to `./tables`
- `-c` | `--config_file` : The Python implementation of a configuration class. See the
    configuration options for details.
- `--no_wipe` : Does not clear the output directories before writing new files.

## 2. Configure the generator
The code generation can be controlled by providing a custom implementation that extends
the `DefaultConfig` class. See the `example_config.py` for some use cases.

### 2.1 Control the mapping between GraphQL and Python
Overwrite or extend the `MAPPING` attribute to map between types not supported by the
`DefaultConfig`. The dictionary has the following form:
- Key: The GraphQL type found in the schema
- Value:
    - A string that represents the Python type
    - A callable that returns the Python type. The function has the following signature:
    `Callable[[ModelModule, tuple[str, GraphQLField]], str]`

Example:
```py
def map_special_type(
    model_module: ModelModule,
    field_info: tuple[str, GraphQLField],
):
    model_module._add_import(
        model_module._imports.external, ".special", "SpecialClass"
    )
    return "SpecialClass"

class CodegenConfig(DefaultConfig):
    MAPPING = {
        **DefaultConfig.MAPPING,
        "unusual_gql_type": "str",
        "special_gql_type": map_special_type,
    }
```

Assuming a GraphQL type definition looks like this:
```gql
type my_table {
    var1: [unusual_gql_type!]!
    var2: special_gql_type
}

```

The generated class would look like this:
```py
from typing import Optional
from .special import SpecialClass

class MyTable(BaseModel):
    _table="my_table"
    var1: Optional[list[str]]
    var2: Optional[SpecialClass]
```

### 2.2. Name your schemas and tables
By overriding the `get_schema_and_table` static method, you can control how each GraphQL
type should be split into schema and table names. The default is to assume only a
`public` schema in which case there is no splitting required.

__Why do I even need this?__
As Hasura combines the (non-public) schema and table names into one GraphQL type name,
it is not possible to automatically split the type name back into its components. If
your Hasura instance exposes any non-public schemas, you have to tell it how to split
the GraphQL type name into the respective schema and table names.

If any other schema than `public` is exposed by the Hasura API, the following
implementation is probably sufficient for most use cases:
```py
    @staticmethod
    def get_schema_and_table(name: str) -> tuple[str, str]:
        # ATTN: this assumes that the schema name has no underscores nor special
        # characters 
        schema, table = name.split("_", maxsplit=1)
        return (schema, table)
```

_Side note_: it is technically possible to implement the `get_schema_and_table` method
in a way so that it connects to the underlying database directly. By running a query
to retrieve the schema and table names from the database, this information could be
retrieved reliably.

### 2.3 Name your generated classes
CuckooCodeGen generates 4 different classes for each database table:

- A base class that only contains the fields of the table, but no relations to other
    tables. This is the return type of `min` and `max` aggregations. Default:
    `TableNameBase`
- A class that only contains the numeric fields of the table. This is the return type of
    most aggregations. Default: `TableNameNumerics`
- A class that represents the aggregation model of the table. This class requires the
    `TableNameBase` and `TableNameNumerics` classes as generics and is the only publicly
    exposed aggregation model. Default: `TableNameAggregate`
- A class that represents the table. Default: `TableName` (camel-casing the table name)

To change the defaults, override the `get_class_names` static method of the following
signature: `Callable[[str], tuple[str, str, str, str]]`

### 2.4 Name your generated files
Override the `get_file_name` static method to chose a file name for each model class
file. By default, the method returns the database table name.

### 2.5 Control aggregate models 
Override the `is_numeric` static method to decide on each GraphQL type, if it is a
numeric Python type. This will decide what properties the model can be aggregated on.

### 2.6 Filter tables
Override the `filter_tables` static method to filter out the desired models to be
generated. Note that models that are excluded by the filter methods, but are declared
as relations in other models will produce "error"-types for those relations.