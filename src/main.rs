use anyhow::{anyhow, Result, Error};
use std::fs::File;
use std::{env, process};
use std::io::{self, Write};
use simple_query_engine::{Query, Table};

fn main() {
    match run() {
        Ok(_) => process::exit(0),
        Err(err) => {
            eprintln!("Error: {}", err);
            process::exit(1);
        }
    }
}

fn run() -> Result<(), Error> {
    let args: Vec<String> = env::args().collect();
    if args.len() < 2 {
        eprintln!("Error: missing the CSV file path argument.");
        eprintln!("Usage: {} <path_to_csv>", args[0]);
        Err(anyhow!("CSV file path not provided"))
    } else {
        let file_path = &args[1];
        let file = File::open(file_path)?;
        let mut reader = csv::Reader::from_reader(file);
        let table = Table::load_from(&mut reader)?;
        let indexed_table = table.build_indices()?;

        loop {
            let stdin = io::stdin();
            let mut input = String::new();
            print!("> ");
            io::stdout().flush()?;
            stdin.read_line(&mut input)?;

            let query = Query::parse(&input)?;

            match simple_query_engine::execute(&query, &indexed_table) {
                Ok(result_set) => {
                    let header = query.column_names.join(",");
                    let header_separator = "-".repeat(header.len());
                    print!("{}\n{}\n", header, header_separator);
                    for row in result_set.rows.iter() {
                        println!("{}", row);
                    }
                },
                Err(err) =>
                    eprintln!("Query execution error: {}", err)
            }
        }
    }
}
