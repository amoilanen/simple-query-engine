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

const EXIT_COMMANDS: [&str; 3] = ["exit", "quit", "q"];

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
        let mut should_exit = false;
        while !should_exit {
            let stdin = io::stdin();
            let mut input = String::new();
            print!("> ");
            io::stdout().flush()?;
            stdin.read_line(&mut input)?;

            if EXIT_COMMANDS.contains(&input.trim()) {
                should_exit = true;
            } else {
                match Query::parse(&input) {
                    Ok(query) =>
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
                    Err(err) =>
                      eprintln!("Query parsing error: {}", err)
                }
            }
        }
        Ok(())
    }
}
