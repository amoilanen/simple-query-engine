use std::cmp::Ordering;
use std::fmt;
use anyhow::{Result, Error};
use crate::table::{IndexedTable, Index};
use crate::query::{FilterType, Filter, Query};
use crate::value::Value;

#[derive(Debug, PartialEq)]
pub struct ResultSetRow {
    pub fields: Vec<Value>
}

impl fmt::Display for ResultSetRow {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        let formatted_fields: Vec<String> = self.fields.iter()
            .map(|field| format!("{}", field)).collect();
        write!(f, "{}", formatted_fields.join(","))
    }
}

#[derive(Debug, PartialEq)]
pub struct ResultSet {
    pub rows: Vec<ResultSetRow>
}

pub fn execute(query: &Query, table: &IndexedTable) -> Result<ResultSet, Error> {
    let row_ids = if let Some(filter) = &query.filter {
        apply_filter(table, filter)?
    } else {
        (0..table.underlying.rows.len()).collect()
    };
    project_rows(table, &row_ids, &query.column_names)
}

fn apply_filter(table: &IndexedTable, filter: &Filter) -> Result<Vec<usize>, Error> {
    if let Some(column_index) = table.indices.column_indices.get(&filter.column_name) {
        filter_using_index(filter, column_index)
    } else {
        filter_by_scanning(table, filter)
    }
}

fn project_rows(table: &IndexedTable, row_ids: &Vec<usize>, column_names: &Vec<String>) -> Result<ResultSet, Error> {
    let mut column_positions: Vec<usize> = Vec::new();
    for column_name in column_names.iter() {
        let column_position = table.underlying.find_column_position(&column_name)?;
        column_positions.push(column_position);
    }
    let mut rows: Vec<ResultSetRow> = Vec::new();
    for row_id in row_ids.into_iter() {
        let projected_row = &table.underlying.rows[*row_id];
        let row_projection: Vec<Value> = column_positions.iter()
            .map(|&column_position| projected_row.fields[column_position].clone())
            .collect();
        rows.push(ResultSetRow {
            fields: row_projection
        });
    }
    Ok(ResultSet { rows })
}

fn filter_using_index(filter: &Filter, index: &Index<'_>) -> Result<Vec<usize>, Error> {
    match filter.filter_type {
        FilterType::Greater => {
            filter_using_index_greater_than(&filter.value, index)
        },
        FilterType::Equal => {
            filter_using_index_equal_to(&filter.value, index)
        }
    }
}

fn filter_using_index_greater_than(value: &Value, index: &Index<'_>) -> Result<Vec<usize>, Error> {
    let mut row_ids: Vec<usize> = Vec::new();
    let found_idx = match index.sorted_column_values
        .binary_search_by(|value_in_row| {
           if *value_in_row.value <= *value {
               Ordering::Less
           } else {
               Ordering::Greater
           }
        }) {
           Err(idx) =>
               if idx < index.sorted_column_values.len() {
                   Some(idx)
               } else {
                   None
               }
           _ => None
        };
    if let Some(first_idx_greater_than) = found_idx {
        row_ids = index.sorted_column_values[first_idx_greater_than..].iter().map(|value_in_row| value_in_row.row_index).collect();
    }
    Ok(row_ids)
}

fn filter_using_index_equal_to(value: &Value, index: &Index<'_>) -> Result<Vec<usize>, Error> {
    let mut row_ids: Vec<usize> = Vec::new();
    if let Some(found_idx) = index.sorted_column_values
        .binary_search_by_key(&value, |value_in_row| value_in_row.value).ok() {
        let mut all_matching_idx = vec![found_idx];
        let mut current_idx = found_idx - 1;
        while current_idx > 0 && index.sorted_column_values[current_idx].value == value {
            all_matching_idx.push(current_idx);
            current_idx = current_idx - 1;
        }
        current_idx = found_idx + 1;
        while current_idx < index.sorted_column_values.len() && index.sorted_column_values[current_idx].value == value {
            all_matching_idx.push(current_idx);
            current_idx = current_idx + 1;
        }
        all_matching_idx.iter().for_each(|&matching_idx| {
            row_ids.push(index.sorted_column_values[matching_idx].row_index);
        });
    }
    Ok(row_ids)
}

fn filter_by_scanning(table: &IndexedTable, filter: &Filter) -> Result<Vec<usize>, Error> {
    let mut row_ids: Vec<usize> = Vec::new();
    let column_position = table.underlying.find_column_position(&filter.column_name)?;
    for (row_id, row) in table.underlying.rows.iter().enumerate() {
        let is_row_matched_by_filter = match filter.filter_type {
            FilterType::Greater => row.fields[column_position] > filter.value,
            FilterType::Equal => row.fields[column_position] == filter.value
        };
        if is_row_matched_by_filter {
            row_ids.push(row_id);
        }
    }
    Ok(row_ids)
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
                "Cannot find column column4, it does not exist in the table, existing columns column1, column2, column3"
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
                "Cannot find column column4, it does not exist in the table, existing columns column1, column2, column3"
            ),
            Ok(_) => panic!("Error expected"),
        }
    }

    #[test]
    fn should_find_all_rows_when_using_equal_filter() {
        let input = r#"column1,column2
a,1
b,2
c,3
d,3
e,3
f,4
"#;
        let mut reader = ReaderBuilder::new().from_reader(Cursor::new(input));
        let table = Table::load_from(&mut reader).unwrap();
        let indexed_table = table.build_indices().unwrap();
        let query = Query::parse("PROJECT column1 FILTER column2 = 3").unwrap();
        let result_set = execute(&query, &indexed_table).unwrap();
        assert_eq!(result_set, ResultSet {
            rows: vec![
                ResultSetRow {
                    fields: vec![Value::Text("d".to_string())]
                },
                ResultSetRow {
                    fields: vec![Value::Text("c".to_string())]
                },
                ResultSetRow {
                    fields: vec![Value::Text("e".to_string())]
                }
            ]
        })
    }

    #[test]
    fn should_correctly_handle_duplicate_filter_column_values_for_greater_filter() {
        let input = r#"column1,column2
a,1
b,2
c,3
d,3
e,3
f,4
"#;
        let mut reader = ReaderBuilder::new().from_reader(Cursor::new(input));
        let table = Table::load_from(&mut reader).unwrap();
        let indexed_table = table.build_indices().unwrap();
        let query = Query::parse("PROJECT column1 FILTER column2 > 3").unwrap();
        let result_set = execute(&query, &indexed_table).unwrap();
        assert_eq!(result_set, ResultSet {
            rows: vec![
                ResultSetRow {
                    fields: vec![Value::Text("f".to_string())]
                }
            ]
        })
    }

    #[test]
    fn should_execute_query_with_two_columns_in_projection_and_equal_filter_matching_no_rows() {
        let table = load_test_table().unwrap();
        let indexed_table = table.build_indices().unwrap();
        let query = Query::parse("PROJECT column1, column2 FILTER column1 = \"hhh\"").unwrap();
        let result_set = execute(&query, &indexed_table).unwrap();
        assert_eq!(result_set, ResultSet {
            rows: Vec::new()
        })
    }
}