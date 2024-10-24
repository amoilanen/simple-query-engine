use anyhow::{anyhow, Result, Error};
use crate::{query::Query, table::IndexedTable};
use crate::value::Value;

#[derive(Debug, PartialEq)]
struct ResultSetRow {
    fields: Vec<Value>
}

#[derive(Debug, PartialEq)]
struct ResultSet {
    rows: Vec<ResultSetRow>
}

pub fn execute(query: &Query, table: &IndexedTable) -> Result<ResultSet, Error> {
    //TODO: Implement
    Ok(ResultSet { rows: Vec::new() })
}

#[cfg(test)]
mod test {
    use super::*;
    use csv::ReaderBuilder;
    use std::io::Cursor;
    use crate::table::{Table, TableIndices};

    #[test]
    fn should_execute_query_with_two_columns_in_projection_and_greater_filter() {
        let input = r#"column1,column2,column3
bbb,3,b
aaa,1,10
ccc,2,11
eee,2,9
ddd,1,5
"#;
        let mut reader = ReaderBuilder::new().from_reader(Cursor::new(input));
        let table: Table = Table::load_from(&mut reader).unwrap();
        let indexed_table = table.build_indices().unwrap();
        let query = Query::parse("PROJECT column1, column2 FILTER column1 > bbb").unwrap();
        let result_set = execute(&query, &indexed_table).unwrap();
        assert_eq!(result_set, ResultSet {
            rows: vec![
                ResultSetRow {
                    fields: vec![Value::Text("ccc".to_string()), Value::Integer(2)]
                },
                ResultSetRow {
                    fields: vec![Value::Text("ddd".to_string()), Value::Integer(1)]
                },
                ResultSetRow {
                    fields: vec![Value::Text("eee".to_string()), Value::Integer(2)]
                }
            ]
        })
    }

    //TODO: Two columns in projection and equal filter
    //TODO: Two columns in projection and no filter
    //TODO: Single column in projection
    //TODO: Non-existing column is used in the projection
    //TODO: Non-existing column is used in the filter
}