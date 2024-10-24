use anyhow::{Result, Error};
use std::fs::File;
use simple_query_engine::{ value, table, query, query_engine };
use simple_query_engine::table::{Table, TableIndices};

fn main() -> Result<(), Error> {
    let file_path = "./examples/data/input.csv";
    let query = "PROJECT city_name, country, dominant_language FILTER population_size > 1000000";
    let file = File::open(file_path)?;
    let mut reader = csv::Reader::from_reader(file);
    let table = Table::load_from(&mut reader)?;
    let indices = TableIndices::build_for(&table)?;
    print!("{:?}", table);
    Ok(())
}
