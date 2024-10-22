use anyhow::{Result, Error};
use std::fs::File;
use table::Table;

mod table;

fn main() -> Result<(), Error> {
    let file_path = "./examples/data/input.csv";
    let file = File::open(file_path)?;
    let mut reader = csv::Reader::from_reader(file);
    let table = Table::from(&mut reader);
    print!("{:?}", table);
    Ok(())
}
