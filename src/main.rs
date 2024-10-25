use anyhow::{Result, Error};
use std::fs::File;
use simple_query_engine::{Query, Table};

fn main() -> Result<(), Error> {
    let file_path = "./examples/data/input.csv";
    let query = Query::parse("PROJECT city_name, country, dominant_language FILTER population_size > 2000000")?;
    let file = File::open(file_path)?;
    let mut reader = csv::Reader::from_reader(file);
    let table = Table::load_from(&mut reader)?;
    let indexed_table = table.build_indices()?;
    let result_set = simple_query_engine::execute(&query, &indexed_table).unwrap();

    let header = query.column_names.join(",");
    let header_separator = "-".repeat(header.len());
    print!("{}\n{}\n", header, header_separator);
    for row in result_set.rows.iter() {
        print!("{}\n", row);
    }
    Ok(())
}
