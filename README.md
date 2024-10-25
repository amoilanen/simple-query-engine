# simple-query-engine

Simple query engine (currently only for a CSV based dataset loaded into memory)

## Installing from sources

```bash
cargo install --path .
```

## Running locally

```bash
simple-query-engine {path_to_csv_file}
```

Queries can be typed in the open interactive REPL session.
To exit the REPL type in "exit".

For example using the CSV file in the `examples/data` directory:

```bash
simple-query-engine ./examples/data/input.csv
```

The following queries might be executed in the interactive REPL session:

```bash
PROJECT city_name, country, dominant_language FILTER population_size > 2000000
```

```bash
PROJECT city_name, population_size FILTER dominant_language = "German"
```

## Development

### Running tests

```bash
cargo test
```

### Running the engine

```bash
cargo run -- ./examples/data/input.csv
```

## Questions

* What were some of the tradeoffs you made when building this and why were these acceptable tradeoffs?
  - All the columns by default have the indices created rather than to make creation of indexes configurable
    Plus: Easier to use the simple query engine, easier implementation
    Minus: With a larger dataset and inserting rows often updating the indexes can become quite slow/drag on the performance of the engine
  - Rather than using a B-tree for a created index a sorted array is used: it is acceptable for an immutable data structure, but if we would start changing 
  the loaded dataset (appending rows) than we would have to use B-tree since it is cheaper to update a B-tree index
    Plus: Simpler implementation
    Minus: Adding an assumption that the dataset is immutable once it has been loaded into memory
  - Everything is loaded into memory and is always operated on in memory assuming that the data structures all will fit into memory at the same time
    Plus: Ease of implementation, speed of query execution (no need to access the disk)
    Minus: Will not scale for larger datasets, query engine will consume too much memory and might eventually crash or become very slow once OS page swapping starts
* Given more time, what improvements or optimizations would you want to add? When would you add them?
  - More robust query parsing with the proper grammar, recursive descent, tokenization and error handling. Might be added when the syntax of the query would become ever more complex
  - Revisit whether it is absolutely necessary to create additonal structs and object wrappers, maybe would profile the query engine and try to reduce the number of unneeded wrappers in some hot spots
  - Other filter operators and allowing to filter by multiple columns
* What changes are needed to accommodate changes to support other data types, multiple filters, or ordering of results?
  - Extending the implementation of the query parsing and the query engine to handle the additional filters and to be able to chain the filters together in the query engine
  - Ordering of results would have to be implemented as a separate stage in the query engine once the result set has been built
  - Implementing "query planning" in the query engine, for example, the most selective filter should be applied first
* What changes are needed to process extremely large datasets
  - Avoiding loading everything into memory. Using some smart data structures containing references to the actual data and even indexes stored on the disk
  - Understanding when auxiliary data structures and indexes should be loaded into memory and when they can be unloaded from it
  - Implement efficient loading and unloading of the auxiliary data structures and indexes into memory
* What do you still need to do to make this code production ready?
  - More tests, especially covering the corner cases in the query engine execution and query parsing
  - Once more careful review, for the core algorithms and for the memory usage and correct reference handling/avoiding copying or cloning things unnecessarily
  - Profiling and performance testing