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
