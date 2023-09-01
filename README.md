# Cuckoo Hasura

*... making the call :P*


**An opinionated *client-side* GraphQL query builder for Hasura.** Allows to issue complex queries and mutations by calling simple function - no strings required (nor attached :). Results are returned as [pydantic models](https://pydantic-docs.helpmanual.io/usage/models/), which means using type-save objects rather than just dictionaries. Exposes a simple python API and it comes with a tiny code-generating executable for generating models from your GQL schema.

Key features:
 - **clean and intuitive** API that is easy and unobstructive to use
 - **speed & memory** efficient, by returning models async or as a
 generator
 - **robust** with a built-in [re-connect option](https://github.com/jd/tenacity) and by passing conditions to related objects securely
 - **fun**, since your code reads just so much nicer :)

## Content

- [Examples](#examples)
- [Getting Started](#getting-started)
- [Usage](#usage)
- [API Reference](#api-reference)

## Examples

```py
# 1. Query with function calls, not long strings
author = Query(Author)
    .one_by_pk(uuid=some_uuid)
    .returning([
        "uuid", "name", Include(Article).many().returning(["title"])
    ])

# 2. Work with pydantic models (classes) instead of untyped dictionaries
assert isinstance(author.uuid, UUID) and isinstance(author.name, str)

# 3. Call yielding and async methods to "finalize" your query
authors_gen = Query(Author).many().yielding()
assert isinstance(authors_gen, typing.Generator)
authors_coro = await Query(Author).many().returning_async()
assert isinstance(authors_coro, typing.Coroutine)

# 4. All variables, including those of sub-queries, will be passed to Hasura as such
author = Query(Author)
    .one_by_pk(uuid="ABC")
    .returning([
        "uuid",
        Include(Article)
            .many(where={"title": {"_eq": "Cuckoo!"} })
            .returning(["title"]),
    ]) # => `variables = {"uuid": "ABC", "title": { "name": {"_eq": "Cuckoo!"} } }`

# 5. Make batched calls in a transaction
with Insert.batch() as BatchInsert, _, _:
    num_author_rows = BatchInsert(Author)
        .many(data=author_data)
        .yield_affected_rows()
    articles = BatchInsert(Article)
        .many(data=article_data)
        .yielding(["title"])
# =>
# mutation Mutation {
#  insert_authors(..) { affectedRows }
#  insert_articles(..) { returning { title } }
# }

# 6. Easy debugging: just stringify cuckoo!
q = Query(Author)
assert str(q) == "query Query { }"
q.one_by_pk(uuid=some_uuid).returning(["uuid"])
assert str(q) == "query Query($uuid: uuid!) { authors_by_pk(uuid: $uuid) { uuid } }"
```
## Getting Started

1. Install Cuckoo
    ```bash
    pip install cuckoo
    ```

2. Write or generate your models

    2.1 Let the code generator query your Hasura schema API to generate the models you need. See the `codegen/README` for more info.

    2.2 Implement models manually by extending `HasuraTableModel` and save them in a schema-based folder structure. See the `tests/fixture/sample_models/public` folder for examples.

## Usage

### 1. Configuration

The first step for using Cuckoo is making sure we can connect to your Hasura instance. The easiest way to provide connection settings to Cuckoo is by simply defining 3 environment variables:
```sh
HASURA_URL=http://hasura:8080/v1/graphql
HASURA_ROLE=admin
HASURA_ADMIN_SECRET=hasura
```

Alternatively, you can provide the connection settings when instantiating your query or mutation. This comes in handy, if you need to connect to different Hasura instances within the same project:
```py
Query(Author, config={
    "url": "http://hasura:8080/v1/graphql"
    "headers": {
        "X-Hasura-Admin-Secret": "admin",
        "X-Hasura-Role": "hasura",
    }
}).one_by_pk(..).returning()
```

### 2. Queries and Mutations

The Cuckoo API consists of 4 classes that allow you to `Query`, `Insert`, `Update` and `Delete` records in Hasura. A fifth `Mutation` class is only useful when the goal is to combine multiple mutations in a single transaction. Finally, there is the `Include` class that can only be used inside certain methods to help including other models - see [section 2.2]() below for details.
Each of the 4 builder classes takes as a first argument the pydantic model that it should return. It is the only required argument to instantiate a builder class.

You start building your query or mutation by calling one of the few methods that these classes expose:

- `Query().one_by_pk()`: Find one record of a table by its primary key.
- `Query().many()`: Find many records by providing a `where` clause and optionally other conditions like `order_by`, `limit` etc.
- `Query().aggregate()`: Calculate an aggregate like `count`, `min`, `max` etc of a set of records.
- `Insert().one()`: Insert one record into a table.
- `Insert().many()`: Insert many records into the same table.
- `Update().one_by_pk()`: Update one record of a table by its primary key.
- `Update().many()`: Update many records of a table by providing a `where` clause and optionally other parameters for dealing with conflicts and appending data.
- `Update().many_distinct()`: The same as `Update().many()`, but accepts a list of inputs. Each input item consists of the data to update and a `where` clause to match any records in a table.
- `Delete().one_by_pk()`: Delete one record of a table by its primary key.
- `Delete().many()`: Delete many records by providing a `where` clause.

Furthermore, each of these classes expose a static `batch()` method that is intended to be used in an execution context. All queries and mutations executed within the context are sent to Hasura in a transaction and results are therefore only available once code execution moves beyond the execution context. The `batch()` method takes the same `config` argument as the class constructors.

For more details on each of these methods, see the API reference.

#### 2.1 Returning fields

All query and mutation methods (with the exception of `Query().aggregate()`) allow you to finish the query with one of the following methods: `returning()`, `returning_async()`, and `yielding()`. All of these methods accept a `columns` parameter to select the fields of a model being returned and it defaults to `["uuid"]` if not provided. While `returning()` returns a model or list of models directly, the `yielding()` method returns a generator that resolves to the requested model. `returning_async()` returns a coroutine and is intended to be used for parallel requests. Note that queries and mutations inside a `batch()` execution context provide **only** the `yielding()` method, as results will not be immediately available.

#### 2.2 Including sub models
In case you would like to select a field that is actually a relation object, relation array or relation aggregate of a sub model, you can use the `Include` class.

As an example, imagine the `Author` and `AuthorDetail` model having a one-to-one relationship. To get an author's UUID and country, you would build a query as follows:

```py
Query(Author).one_by_pk(uuid="ABC").returning([
    "uuid",
    Include(AuthorDetail).one().returning(["country"]),
])
```

Similarly, provided that the `Author` and the `Article` model have a one-to-many relationship, an insert could look like this:

```py
Insert(Author).one(data={..}).returning([ # `data` would be a nested object with a list of article data
    "uuid",
    Include(Article).many().returning(["uuid"]),
])
```

Finally, we can include sub model aggregates as well. Here an example of getting the average number of words of all the authors articles when updating an author by UUID:
```py
Update(Author).one_by_pk(uuid="ABC", data={..}).returning([
    "uuid",
    Include(Article).aggregate().on(avg={"word_count"}),
])
```

Note that the `Include` class can act as a GraphQL Fragment when being wrapped in a function:
```py
avg_number_of_words = lambda: Include(Article).aggregate().on(avg={"word_count"})

new_author = Insert(Author).one(data={..}).returning(["uuid", avg_number_of_words()])
updated_author = Update(Author).one_by_pk(uuid=.., data={..}).returning(["uuid", avg_number_of_words()])
```

#### 2.3 Aggregations

`Query().aggregate()` and `Include().aggregate()` allow you to calculate aggregates of a set of models or sub models respectively. To retrieve the aggregate you are looking for, the queries needs to be build in 2 steps:

1. By calling `aggregate()` on the builder instance, you can specify a `where` clause, `limit` the number of records to aggregate on and so forth. It has the same purpose as `Query().many()`.
2. The next step is to call `on()` (or `yield_on()`) for specifying what _kind_ of aggregate you are looking for. In case you just want the number of aggregated records, you can call it with `on(count=True)` and would get back an integer. For all other aggregation methods, you need to provide at least one field name (of a numeric field!) to aggregate on. To get the maximum of some numeric field, for example, you would call `on(max={"field_name"})`. Its perfectly fine to use more than one aggregation function.

By default, the returning object of an aggregate query (except for `count`) is an untyped object:
```py
aggregates = Query(Comment).aggregate(where=..).on(max={"likes"}, min={"likes"})
assert isinstance(aggregates.max, Any)
assert isinstance(aggregates.max.likes, float)
```

If you prefer typed objects to be returned from aggregate functions, you need to tell Cuckoo how they are structured. Let's build our model the following way:
```py
class CommentBase(BaseModel):
    uuid: Optional[UUID]
    article_uuid: Optional[UUID]
    content: Optional[str]
    likes: Optional[int]

class CommentNumerics(BaseModel):
    likes: Optional[float]

class Comment(HasuraTableModel, CommentBase):
    _table_name = "comments"
    article: Optional[Article]
```

For the `min` and `max` aggregations, the returned object is the model without any sub model relations, so `CommentBase` in our case. Provide it as the `base_model` argument to get a typed object back:
```py
aggregates = Query(Comment, base_model=CommentBase).aggregate(where=..).on(max={"likes"})
assert isinstance(aggregates.max, CommentBase)
assert isinstance(aggregates.max.content, str)
```

For all other aggregations, the returned object is a model containing only its numeric fields. The type of the numeric fields needs to be `float`. This is `CommentNumerics` in our example. Provide it as the `numeric_model` argument to get the correct object back:
```py
aggregates = Query(Comment, numeric_model=CommentNumerics).aggregate(where=..).on(avg={"likes"})
assert isinstance(aggregates.avg, CommentNumerics)
assert isinstance(aggregates.avg.likes, float)
```

Finally, you can ask for the aggregated models (Hasura calls them `nodes`) to be included in the response. Call the `with_nodes()` (or `yield_with_nodes()`) method on the `Query().aggregate()` builder instead of the `on()` method to do so. Here a simple example:

```py
aggregates, nodes = Query(Comment).aggregate(where=..).with_nodes(
    aggregates={
        "avg": {"likes"}
    },
    columns=["uuid", Include(Article).one().returning(["uuid"])]
)
assert isinstance(aggregates.avg.likes, float)
assert isinstance(nodes[0], Comment)
assert isinstance(nodes[0].article, Article)
```

As you can see from the above example, the `columns` argument accepts the same input as the `columns` argument of the `returning()` method. This means you can use `Include` to include sub models when asking for nodes of an aggregation.

## API reference

### *class* cuckoo.**Query**(*model: Type[TMODEL], config: dict = DEFAULT_CONFIG, logger: Logger = None, base_model: Type[TMODEL_BASE] = UntypedModel, numeric_model: Type[TNUM_PROPS] = UntypedModel*)

The builder for making Hasura queries.

*Args:*
- `model`: The pydantic model of the return value
- *optional* `config`: The connection details to the Hasura server and other configurations
- *optional* `logger`: logs the query and any errors
- *optional* `base_model`: The model (without any relations) to be used for `min` and `max` aggregation results. Defaults to a blank model with `extra="allow"` for easy, but untyped access. 
- *optional* `numeric_model`: A model that contains only the numeric properties of the base model as floats to return all aggregate results, except those of `min` and `max`. Defaults to a blank model with `extra="allow"` for easy, but untyped access.
#### Query.**many**(*where: Dict[str, Any] = None, distinct_on: Set[str] = None, limit: int = None, offset: int = None, order_by: Dict[str, "asc"|"desc"] = None*) -> cuckoo.[ReturningFinalizer](#class-cuckooreturningfinalizer)[list[TMODEL]] | cuckoo.[YieldingFinalizer](#class-cuckooyieldingfinalizer)[list[TMODEL]]

Build a query for a list of models.

  *Args:*
   - *optional* `where`: The where clause to filter the result set with
   - *optional* `distinct_on`: The distinct clause to the query
   - *optional* `limit`: The maximum number of results returned
   - *optional* `offset`: The offset being skipped from the result set
   - *optional* `order_by`: The order-by clause to the query

  *Returns:*
  - *class* cuckoo.[ReturningFinalizer](#class-cuckooreturningfinalizer)[list[TMODEL]] for queries outside of a `batch()` execution context
  - *class* cuckoo.[YieldingFinalizer](#class-cuckooyieldingfinalizer)[list[TMODEL]] for queries inside a `batch()` execution context

#### Query.**one_by_pk**(*uuid: UUID*) -> cuckoo.[ReturningFinalizer](#class-cuckooreturningfinalizer)[TMODEL] | cuckoo.[YieldingFinalizer](#class-cuckooyieldingfinalizer)[TMODEL]


  Build a query for finding a single model by its UUID.

  *Args:*
  - `uuid`: The UUID to query for

  *Returns:*
  - *class* cuckoo.[ReturningFinalizer](#class-cuckooreturningfinalizer)[TMODEL] for queries outside of a `batch()` execution context
  - *class* cuckoo.[YieldingFinalizer](#class-cuckooyieldingfinalizer)[TMODEL] for queries inside a `batch()` execution context

  *Raises:*
  - `NotFoundError` if no result was found

#### Query.**aggregate**(*where: Dict[str, Any] = None, distinct_on: Set[str] = None, limit: int = None, offset: int = None, order_by: Dict[str, "asc"|"desc"] = None*) -> cuckoo.[AggregateFinalizer](#class-cuckooaggregatefinalizer) | cuckoo.[YieldingAggregateFinalizer](#class-cuckooyieldingaggregatefinalizer)
  
  Build an aggregate query.
  
  *Args:*
   - *optional* `where`: The where clause to filter
   - *optional* `distinct_on`: The distinct clause
   - *optional* `limit`: The maximum number of records
   - *optional* `offset`: The offset being skipped from matching records
   - *optional* `order_by`: The order-by clause

  *Returns:*
  - *class* cuckoo.[AggregateFinalizer](#class-cuckooaggregatefinalizer) for queries outside of a `batch()` execution context
  - *class* cuckoo.[YieldingAggregateFinalizer](#class-cuckooyieldingaggregatefinalizer) for queries inside a `batch()` execution context

#### Query.**sql_function**(*function_name: str, args: dict[str, Any] = None, where: WHERE = None, distinct_on: set[str] = None, limit: int = None, offset: int = None, order_by: ORDER_BY = None*)
  
  Build a query for a custom defined function. Note that this method always returns a list of models, even if the function just returns a single row.

  *Args:*
   - `function_name`: The name of the custom function to query
   - *optional* `args`: The arguments passed to the function. Note that the utility function `utils.to_sql_function_args()` is used to convert input into the expected format. 
   - *optional* `where`: The where clause to filter
   - *optional* `distinct_on`: The distinct clause
   - *optional* `limit`: The maximum number of records
   - *optional* `offset`: The offset being skipped from matching records
   - *optional* `order_by`: The order-by clause

  *Returns:*

#### *static* Query.**batch**(*config: dict = DEFAULT_CONFIG, logger: Logger = None*) -> Generator[BatchQuery, None, None]

Returns an execution context for running multiple queries in a transaction. This method is to be used in a `with` statement and returns a transaction-bound `Query` constructor. Any instance of that constructor executes its query in the same transaction. Note that the query is submitted to Hasura on closing the execution context. Hence, the queries only support results as generators, as delivery of the results is delayed. Example:
```py
with Query.batch() as BatchQuery:
    authors_over_50 = BatchQuery(Author)
        .many(where={"age": {"_gt": 50} })
        .yielding(["age"])
    next(authors_over_50) # ERROR! The query has not been executed yet!
next(authors_over_50) # OK
```

#### *static* Query.**batch_async**(*config: dict = DEFAULT_CONFIG, logger: Logger = None*) -> Generator[BatchQuery]

A asynchronous version of `Query.batch()`. Example:
```py
async def get_authors():
    async with Query.batch_async() as BatchQuery:
        authors_over_50 = BatchQuery(Author).many(where={"age": {"_gt": 50}}).yielding()
        special_author = BatchQuery(Author).one_by_pk(uuid=special_uuid).yielding()
    return authors_over_50, next(special_author)

authors_over_50, special_author = await get_authors()
for author_over_50 in authors_over_50:
    assert special_author.uuid != author_over_50.uuid
```


### *class* cuckoo.**Insert**(*model: Type[TMODEL], config: dict = DEFAULT_CONFIG, logger: Logger = None*)

#### Insert.**many**(*data: list[dict[str, Any]] = None, on_conflict: OnConflict = None*):

#### Insert.**one**(*data: dict[str, Any] = None, on_conflict: dict[str, str] = None*):
 
### *class* cuckoo.**Update**(*model: Type[TMODEL], config: dict = DEFAULT_CONFIG, logger: Logger = None*)

#### Update.**many**(*where: dict, data: dict = None, inc: dict = None, append: dict = None, prepend: dict = None, delete_key: dict = None, delete_elem: dict = None, delete_at_path: dict = None*):

#### Update.**many_distinct**(*updates: list[dict]*):

Better method naming?

#### Update.**one_by_pk**(*pk_columns: dict = None, data: dict = None, append: dict = None, delete_at_path: dict = None, delete_elem: dict = None, delete_key: dict = None, inc: dict = None, prepend: dict = None*):

### *class* cuckoo.**Delete**(*model: Type[TMODEL], config: dict = DEFAULT_CONFIG, logger: Logger = None*)

#### Delete.**many**(*where: dict = None*):

#### Delete.**one_by_pk**(*uuid: UUID*):


### *class* cuckoo.**Mutation**

#### *static* Mutation.**batch**(*config: dict = None, logger: Optional[Logger] = None*):

Returns an execution context for running multiple mutations in a transaction. This method is to be used in a `with` statement and returns a tuple of transaction-bound constructors: `(Insert, Update, Delete)`. Any instance of that constructor executes its query in the same transaction. Note that the query is submitted to Hasura on closing the execution context. Hence, the queries only support results as generators, as delivery of the results is delayed. Example:
```py
with Mutation.batch() as BatchInsert, BatchUpdate, _ :
    num_author_rows = BatchInsert(Author)
        .many(data=author_data)
        .yield_affected_rows()
    articles = BatchUpdate(Article)
        .many(data=article_data, where={"created_at": { "_lt": "now" } })
        .yielding(["title"])
    next(num_author_rows) # ERROR! The query has not been executed yet!
next(num_author_rows) # OK
```

#### *static* Mutation.**batch_async**(*config: dict = None, logger: Optional[Logger] = None*):

A asynchronous version of `Mutation.batch()`. Example:
```py
async def do_in_transaction():
    async with Mutation.batch_async() as BatchInsert, BatchUpdate, _ :
        num_author_rows = BatchInsert(Author)
            .many(data=author_data)
            .yield_affected_rows()
        articles = BatchUpdate(Article)
            .many(data=article_data, where={"created_at": { "_lt": "now" } })
            .yielding(["title"])
    return next(num_author_rows), list(articles)

num_author_rows, articles = await do_in_transaction()
```

### *class* cuckoo.**Include**(*model: Type[TMODEL], key: str = None*)

#### Include.**many**(*where: WHERE = None, distinct_on: set[str] = None, limit: int = None, offset: int, order_by: ORDER_BY = None*)

#### Include.**one**()

#### Include.**aggregate**(*where: WHERE = None, distinct_on: set[str] = None, limit: int = None, offset: int = None, order_by: ORDER_BY = None*)

----
### *class* cuckoo.**YieldingFinalizer**

Provide `yielding` method to finalize queries that require a list of columns.

#### YieldingFinalizer.**yielding**(*columns: list[str | TINCLUDE] = ["uuid"]*) -> Generator[TMODEL]

  Get a generator that resolves to the result of the query with the columns provided.

  *Args:*
   - `columns`: The columns of the result object

  *Returns:*
   - Generator resolving to a list or a single model

### *class* cuckoo.**ReturningFinalizer**

*extends:* [YieldingFinalizer](#class-cuckooyieldingfinalizer)

Provide `returning` / `yielding` methods to finalize queries that require a list of columns.

#### ReturningFinalizer.**returning**(*columns: list[str | TINCLUDE] = ["uuid"]*) -> TMODEL | list[TMODEL]

  Get the result of the query with the columns provided.

  *Args:*
   - `columns`: The columns of the result object

  *Returns:*
   - a list or a single model

#### ReturningFinalizer.**returning_async**(*columns: list[str | TINCLUDE] = ["uuid"]*) -> TMODEL | list[TMODEL]

  An asynchronous version of `ReturningFinalizer.returning()`.

### *class* cuckoo.**YieldingAffectedRowsFinalizer**

*extends:* [YieldingFinalizer](#class-cuckooyieldingfinalizer)

Provide `yielding`, `yield_affected_rows`, `yielding_with_rows` methods to finalize queries that either return a number, a list of models, or both.

#### YieldingAffectedRowsFinalizer.**yield_affected_rows**()

  Get a generator that resolves to the number of affected rows.

  *Args:*
   - `columns`: The columns of the result object

  *Returns:*
   - a list or a single model

#### YieldingAffectedRowsFinalizer.**yielding_with_rows**(*columns: list[str | TINCLUDE] = ["uuid"]*) -> tuple[Generator[TMODEL], Generator[int]]

  Get the result of the query with the columns provided as well as the number of affected rows.

  *Args:*
   - `columns`: The columns of the result object

  *Returns:*
   - a tuple of 2 generators, the first resolves to a list of model, the second to the number of affected rows

### *class* cuckoo.**AffectedRowsFinalizer**
*extends:* [YieldingFinalizer](#class-cuckooyieldingfinalizer), [YieldingAffectedRowsFinalizer](#class-cuckooyieldingaffectedrowsfinalizer)

Provide `returning`, `affected_rows`, `returning_with_rows` methods (on top of their yielding counter parts) to finalize queries that either return a number, a list of models, or both.

#### AffectedRowsFinalizer.**affected_rows**() -> int

  Get the number of affected rows.

  *Returns:*
   - the number of affected rows.

#### AffectedRowsFinalizer.**affected_rows_async**() -> int

  An asynchronous version of `AffectedRowsFinalizer.affected_rows()`.

#### AffectedRowsFinalizer.**returning_with_rows**(*columns: list[str | "TINCLUDE"] = ["uuid"]*)

  Get the result of the query with the columns provided as well as the number of affected rows.

  *Args:*
   - `columns`: The columns of the result object

  *Returns:*
   - a tuple with the first element being a list of models and the second being the number of affected rows

#### AffectedRowsFinalizer.**returning_with_rows_async**() -> int

  An asynchronous version of `AffectedRowsFinalizer.returning_with_rows()`.

### *class* cuckoo.**YieldingAggregateFinalizer**

Provide `yield_on` and `yield_with_nodes` methods to finalize queries that return aggregates or aggregates along with their nodes.

#### YieldingAggregateFinalizer.**yield_on**(*count: Union[bool, CountDict] = None, avg: set[str] = None, max: set[str] = None, min: set[str] = None, stddev: set[str] = None, stddev_pop: set[str] = None, stddev_samp: set[str] = None, sum: set[str] = None, var_pop: set[str] = None, var_samp: set[str] = None, variance: set[str] = None*)

Get a generator that yields an aggregate model.

  *Args:*
   - *optional* `count`: Either `True` or a dict indicating the column to count
   - *optional* `avg`: A set of column names to avarage on
   - *optional* `max`: A set of column names to find the maximum for
   - *optional* `min`: A set of column names to find the minimum for
   - *optional* `stddev`: A set of column names to calculate the standard deviation on
   - *optional* `stddev_pop`: A set of column names to calculate the standard deviation on
   - *optional* `stddev_samp`: A set of column names to calculate the standard deviation on
   - *optional* `sum`: A set of column names that should be summed up
   - *optional* `var_pop`: A set of column names to calculate the variance on
   - *optional* `var_samp`: A set of column names to calculate the variance on
   - *optional* `variance`: A set of column names to calculate the variance on

  *Returns:*
  - A generator that yields an [AggregateModel](#). Each provided aggregate argument of this method populates the corresponding property of the returning aggregate model.

#### YieldingAggregateFinalizer.**yield_with_nodes**(*aggregates: AggregatesDict, nodes: list[dict]*)

  *Args:*
   - `aggregates`: The aggregates with their respective column(s) to do the aggregation on
   - `nodes`: The columns of the nodes that should be returned

  *Returns:*
  - an [AggregateModel](#) with the selected memebers being populated 

### *class* cuckoo.**AggregateFinalizer**
#### AggregateFinalizer.**on**(*count: Union[bool, CountDict] = None, avg: set[str] = None, max: set[str] = None, min: set[str] = None, stddev: set[str] = None, stddev_pop: set[str] = None, stddev_samp: set[str] = None, sum: set[str] = None, var_pop: set[str] = None, var_samp: set[str] = None, variance: set[str] = None*)

  *see YieldingAggregateFinalizer.yield_on()*

#### AggregateFinalizer.**on_async**(*count: Union[bool, CountDict] = None, avg: set[str] = None, max: set[str] = None, min: set[str] = None, stddev: set[str] = None, stddev_pop: set[str] = None, stddev_samp: set[str] = None, sum: set[str] = None, var_pop: set[str] = None, var_samp: set[str] = None, variance: set[str] = None*)

  An asynchronous version of `AggregateFinalizer.on()`.

#### AggregateFinalizer.**with_nodes**(*aggregates: AggregatesDict, nodes: list[dict]*)

  *see YieldingAggregateFinalizer.yield_with_nodes()*

#### AggregateFinalizer.**with_nodes_async**(*aggregates: AggregatesDict, nodes: list[dict]*)

  An asynchronous version of `AggregateFinalizer.with_nodes()`.

#### AggregateFinalizer.**count**(*columns: set[str] = None, distinct: bool = None*) -> int

#### AggregateFinalizer.**avg**(*columns: set[str]*) -> float

#### AggregateFinalizer.**max**(*columns: set[str]*) -> float

#### AggregateFinalizer.**min**(*columns: set[str]*) -> float

#### AggregateFinalizer.**sum**(*columns: set[str]*) -> float

## Wishlist

- type checker
- maybe more syntactic sugar, like having `where` and `order_by` methods (?):
```py
# Build queries dynamically
def get_by_A_or_b(a=None, b=None):
    q = Query(Author).query_all(where={"deleted": {"_eq": False}})
    if a:
        q.and_where({"a": {"_eq" :a} }).order_by({"a": "asc"})
    elif b:
        q.and_where({"b": {"_eq": b} }).order_by({"b": "desc"})
    else:
        raise ValueError("Need either a or b!")
    return q.returning()


# query/insert large data sets in batches
# TODO: think of a suiting API
```