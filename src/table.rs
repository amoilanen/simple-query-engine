use anyhow::{anyhow, Result, Error};
use std::collections::HashMap;
use csv;
use crate::value::Value;

#[derive(Debug, PartialEq)]
pub struct Table {
    columns: Vec<Column>,
    rows: Vec<Row>
}

#[derive(Debug, PartialEq)]
pub struct TableIndices<'a> {
    column_indices: HashMap<String, Index<'a>>
}

#[derive(Debug, PartialEq)]
pub struct Index<'a> {
    column_name: String,
    sorted_column_values: Vec<ValueInRow<'a>>
}

#[derive(Debug, PartialEq)]
pub struct ValueInRow<'a> {
    value: &'a Value,
    row_index: usize
}

#[derive(Debug, PartialEq)]
pub struct Column {
    name: String,
    column_type: ColumnType
}

#[derive(Debug, PartialEq)]
pub enum ColumnType {
    Integer,
    Text
}

#[derive(Debug, PartialEq)]
pub struct Row {
    fields: Vec<Value>
}

impl TableIndices<'_> {
    pub fn build_for(table: &Table) -> Result<TableIndices, Error> {
        let mut column_indices: HashMap<String, Index> = HashMap::new();
        for (column_index, column) in table.columns.iter().enumerate() {
            let column_name = column.name.to_string();
            let mut column_values: Vec<ValueInRow> = Vec::new();
            for (row_index, row) in table.rows.iter().enumerate() {
                let column_value = row.fields.get(column_index).ok_or_else(|| anyhow!("Row {:?} does not have column {:?}", &row, &column_name))?;
                column_values.push(ValueInRow {
                    value: column_value,
                    row_index
                });
            }
            column_values.sort_by(|x, y| x.value.cmp(y.value));
            column_indices.insert(column_name.to_string(), Index {
                column_name,
                sorted_column_values: column_values
            });
        }
        Ok(TableIndices {
            column_indices
        })
    }
}

impl Table {

    pub fn load_from<R: std::io::Read>(reader: &mut csv::Reader<R>) -> Result<Table, Error> {
        let rows = Table::parse_rows(reader)?;
        let columns = Table::parse_columns(reader, &rows)?;
        Ok(Table {
            columns,
            rows
        })
    }

    fn parse_rows<R: std::io::Read>(reader: &mut csv::Reader<R>) -> Result<Vec<Row>, Error> {
        let mut rows: Vec<Row> = Vec::new();
        for record in reader.records() {
            let mut fields: Vec<Value> = Vec::new();
            for column in record?.into_iter() {
                let field = Value::parse_value(column.to_string())?;
                fields.push(field);
            }
            rows.push(Row {
                fields
            })
        }
        Ok(rows)
    }

    fn parse_columns<R: std::io::Read>(reader: &mut csv::Reader<R>, rows: &Vec<Row>) -> Result<Vec<Column>, Error> {
        let headers: Vec<String> = reader.headers()?.into_iter().map(|header| header.to_string()).collect();
        let mut columns: Vec<Column> = Vec::new();
        for (index, header) in headers.into_iter().enumerate() {
            let mut column_values: Vec<&Value> = Vec::new();
            for row in rows.iter() {
                let row_field = row.fields.get(index).ok_or_else(|| anyhow!("Row {:?} does not have column {:?}", &row, &header))?;
                column_values.push(row_field);
            }
            let is_integer_column = column_values.into_iter().all(|field| match field {
                Value::Integer(_) => true,
                Value::Text(_) => false
            });
            let column_type = if is_integer_column {
                ColumnType::Integer
            } else {
                ColumnType::Text
            };
            let column = Column {
                name: header,
                column_type
            };
            columns.push(column);
        }
        Ok(columns)
    }
}

#[cfg(test)]
mod test {
    use super::*;
    use csv::ReaderBuilder;
    use std::io::Cursor;

    #[test]
    fn should_load_table_from_csv() {
        let input = r#"column1,column2,column3
bbb,3,b
aaa,1,10
ccc,2,11"#;
        let mut reader = ReaderBuilder::new().from_reader(Cursor::new(input));
        let table = Table::load_from(&mut reader).unwrap();
        assert_eq!(table, Table {
            columns: vec![
                Column {
                    name: "column1".to_string(),
                    column_type: ColumnType::Text
                },
                Column {
                    name: "column2".to_string(),
                    column_type: ColumnType::Integer
                },
                Column {
                    name: "column3".to_string(),
                    column_type: ColumnType::Text
                }
            ],
            rows: vec![
                Row {
                    fields: vec![Value::Text("bbb".to_string()), Value::Integer(3), Value::Text("b".to_string())]
                },
                Row {
                    fields: vec![Value::Text("aaa".to_string()), Value::Integer(1), Value::Integer(10)]
                },
                Row {
                    fields: vec![Value::Text("ccc".to_string()), Value::Integer(2), Value::Integer(11)]
                }
            ]
        })
    }

    #[test]
    fn should_build_indices_for_table() {
        let input = r#"column1,column2,column3
bbb,3,b
aaa,1,10
ccc,2,11"#;
        let mut reader = ReaderBuilder::new().from_reader(Cursor::new(input));
        let table = Table::load_from(&mut reader).unwrap();
        let indices = TableIndices::build_for(&table).unwrap();
        let aaa = Value::Text("aaa".to_string());
        let bbb = Value::Text("bbb".to_string());
        let ccc = Value::Text("ccc".to_string());
        let b = Value::Text("b".to_string());
        assert_eq!(indices, TableIndices {
            column_indices: {
                let mut columns_indices = HashMap::new();
                columns_indices.insert("column1".to_string(), Index {
                    column_name: "column1".to_string(),
                    sorted_column_values: vec![
                        ValueInRow { value: &aaa, row_index: 1 },
                        ValueInRow { value: &bbb, row_index: 0 },
                        ValueInRow { value: &ccc, row_index: 2 }
                    ]
                });
                columns_indices.insert("column2".to_string(), Index {
                    column_name: "column2".to_string(),
                    sorted_column_values: vec![
                        ValueInRow { value: &Value::Integer(1), row_index: 1 },
                        ValueInRow { value: &Value::Integer(2), row_index: 2 },
                        ValueInRow { value: &Value::Integer(3), row_index: 0 }
                    ]
                });
                columns_indices.insert("column3".to_string(), Index {
                    column_name: "column3".to_string(),
                    sorted_column_values: vec![
                        ValueInRow { value: &Value::Integer(10), row_index: 1 },
                        ValueInRow { value: &Value::Integer(11), row_index: 2 },
                        ValueInRow { value: &b, row_index: 0 },
                    ]
                });
                columns_indices
            }
        })
    }
}