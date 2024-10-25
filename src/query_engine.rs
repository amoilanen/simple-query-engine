use std::cmp::Ordering;
use anyhow::{anyhow, Result, Error};
use crate::table::IndexedTable;
use crate::query::{FilterType, Query};
use crate::value::Value;

#[derive(Debug, PartialEq)]
pub struct ResultSetRow {
    fields: Vec<Value>
}

#[derive(Debug, PartialEq)]
pub struct ResultSet {
    rows: Vec<ResultSetRow>
}

//TODO: Re-factor
pub fn execute(query: &Query, table: &IndexedTable) -> Result<ResultSet, Error> {
    //TODO: Split a function to find the row ids
    let mut row_ids: Vec<usize> = Vec::new();
    if let Some(filter) = &query.filter {
        let filter_column_ord = table.underlying.columns.iter()
            .position(|column| column.name == filter.column_name)
            .ok_or_else(|| anyhow!("Cannot filter by column {}, it does not exist in the table, existing columns {}",
                filter.column_name,
                table.underlying.column_names().join(", "))
            )?;
        if let Some(column_index) = table.indices.column_indices.get(&filter.column_name) {
            match filter.filter_type {
                FilterType::Greater => {
                    let found_idx = match column_index.sorted_column_values
                        .binary_search_by(|value_in_row| {
                           if *value_in_row.value <= filter.value {
                               Ordering::Less
                           } else {
                               Ordering::Greater
                           }
                        }) {
                           Err(idx) =>
                               if idx < column_index.sorted_column_values.len() {
                                   Some(idx)
                               } else {
                                   None
                               }
                           _ => None
                        };

                    if let Some(first_idx_greater_than) = found_idx {
                        row_ids = column_index.sorted_column_values[first_idx_greater_than..].iter().map(|value_in_row| value_in_row.row_index).collect();
                    }
                },
                FilterType::Equal => {
                    if let Some(found_row) = column_index.sorted_column_values
                        .binary_search_by_key(&&filter.value, |value_in_row| value_in_row.value).ok()
                        .and_then(|idx| column_index.sorted_column_values.get(idx)) {
                        row_ids.push(found_row.row_index)
                    }
                }
            };
        } else {
            //No index found on the filtering column => scan all rows and filter all rows inefficiently
            for (row_id, row) in table.underlying.rows.iter().enumerate() {
                let is_row_matched_by_filter = match filter.filter_type {
                    FilterType::Greater => row.fields[filter_column_ord] > filter.value,
                    FilterType::Equal => row.fields[filter_column_ord] == filter.value
                };
                if is_row_matched_by_filter {
                    row_ids.push(row_id);
                }
            }
        }
    } else {
        // No filter: return all rows
        row_ids = (0..table.underlying.rows.len()).collect();
    }

    //TODO: Split a function to project the found row_ids
    let mut projection_column_ords: Vec<usize> = Vec::new();
    for projection_column_name in query.column_names.iter() {
        let projection_column_ord = table.underlying.columns.iter()
            .position(|column| column.name == projection_column_name.to_string())
            .ok_or_else(|| anyhow!("Cannot project column {}, it does not exist in the table, existing columns {}",
                projection_column_name,
                table.underlying.column_names().join(", "))
            )?;
        projection_column_ords.push(projection_column_ord);
    }
    let mut rows: Vec<ResultSetRow> = Vec::new();
    for row_id in row_ids.into_iter() {
        let row_to_project = &table.underlying.rows[row_id];
        let row_fields: Vec<Value> = projection_column_ords.iter().map(|&column_ord| row_to_project.fields[column_ord].clone()).collect();
        rows.push(ResultSetRow {
            fields: row_fields
        });
    }
    Ok(ResultSet { rows })
}

#[cfg(test)]
mod test {
    use super::*;
    use csv::ReaderBuilder;
    use std::io::Cursor;
    use crate::table::Table;

    fn load_test_table() -> Result<Table, Error> {
        let input = r#"column1,column2,column3
bbb,3,b
aaa,1,10
ccc,2,11
eee,2,9
ddd,1,5
"#;
        let mut reader = ReaderBuilder::new().from_reader(Cursor::new(input));
        Table::load_from(&mut reader)
    }

    #[test]
    fn should_execute_query_with_two_columns_in_projection_and_greater_filter() {
        let table = load_test_table().unwrap();
        let indexed_table = table.build_indices().unwrap();
        let query = Query::parse("PROJECT column1, column2 FILTER column1 > \"bbb\"").unwrap();
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

    #[test]
    fn should_execute_query_with_two_columns_in_projection_and_equal_filter() {
        let table = load_test_table().unwrap();
        let indexed_table = table.build_indices().unwrap();
        let query = Query::parse("PROJECT column1, column2 FILTER column3 = 9").unwrap();
        let result_set = execute(&query, &indexed_table).unwrap();
        assert_eq!(result_set, ResultSet {
            rows: vec![
                ResultSetRow {
                    fields: vec![Value::Text("eee".to_string()), Value::Integer(2)]
                }
            ]
        })
    }

    #[test]
    fn should_execute_query_with_two_columns_in_projection_and_no_filter() {
        let table = load_test_table().unwrap();
        let indexed_table = table.build_indices().unwrap();
        let query = Query::parse("PROJECT column1, column2").unwrap();
        let result_set = execute(&query, &indexed_table).unwrap();
        assert_eq!(result_set, ResultSet {
            rows: vec![
                ResultSetRow {
                    fields: vec![Value::Text("bbb".to_string()), Value::Integer(3)]
                },
                ResultSetRow {
                    fields: vec![Value::Text("aaa".to_string()), Value::Integer(1)]
                },
                ResultSetRow {
                    fields: vec![Value::Text("ccc".to_string()), Value::Integer(2)]
                },
                ResultSetRow {
                    fields: vec![Value::Text("eee".to_string()), Value::Integer(2)]
                },
                ResultSetRow {
                    fields: vec![Value::Text("ddd".to_string()), Value::Integer(1)]
                }
            ]
        })
    }

    #[test]
    fn should_execute_query_with_two_columns_in_projection_and_filter_matching_no_rows() {
        let table = load_test_table().unwrap();
        let indexed_table = table.build_indices().unwrap();
        let query = Query::parse("PROJECT column1, column2 FILTER column1 > \"eee\"").unwrap();
        let result_set = execute(&query, &indexed_table).unwrap();
        assert_eq!(result_set, ResultSet {
            rows: Vec::new()
        })
    }

    #[test]
    fn should_execute_query_with_single_column_in_projection() {
        let table = load_test_table().unwrap();
        let indexed_table = table.build_indices().unwrap();
        let query = Query::parse("PROJECT column1 FILTER column2 > 2").unwrap();
        let result_set = execute(&query, &indexed_table).unwrap();
        assert_eq!(result_set, ResultSet {
            rows: vec![
                ResultSetRow {
                    fields: vec![Value::Text("bbb".to_string())]
                }
            ]
        })
    }

    #[test]
    fn should_produce_error_when_non_existent_column_is_used_in_projection() {
        let table = load_test_table().unwrap();
        let indexed_table = table.build_indices().unwrap();
        let query = Query::parse("PROJECT column4 FILTER column2 > 2").unwrap();
        let result = execute(&query, &indexed_table);
        match result {
            Err(e) => assert_eq!(
                e.to_string(),
                "Cannot project column column4, it does not exist in the table, existing columns column1, column2, column3"
            ),
            Ok(_) => panic!("Error expected"),
        }
    }

    #[test]
    fn should_produce_error_when_non_existent_column_is_used_in_filter() {
        let table = load_test_table().unwrap();
        let indexed_table = table.build_indices().unwrap();
        let query = Query::parse("PROJECT column1 FILTER column4 > 2").unwrap();
        let result = execute(&query, &indexed_table);
        match result {
            Err(e) => assert_eq!(
                e.to_string(),
                "Cannot filter by column column4, it does not exist in the table, existing columns column1, column2, column3"
            ),
            Ok(_) => panic!("Error expected"),
        }
    }
}