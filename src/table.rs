use anyhow::{anyhow, Result, Error};
use csv::{self, StringRecord};

#[derive(Debug, PartialEq)]
pub(crate) struct Table {
    columns: Vec<Column>,
    rows: Vec<Row>
}

#[derive(Debug, PartialEq)]
pub(crate) struct Column {
    name: String,
    column_type: ColumnType
}

#[derive(Debug, PartialEq)]
pub(crate) enum ColumnType {
    Integer,
    Text
}

#[derive(Debug, PartialEq)]
pub(crate) struct Row {
    fields: Vec<Field>
}

#[derive(Debug, PartialEq)]
pub(crate) enum Field {
    Integer(u64),
    Text(String)
}

impl Table {

    pub(crate) fn from<R: std::io::Read>(reader: &mut csv::Reader<R>) -> Result<Table, Error> {
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
            let mut fields: Vec<Field> = Vec::new();
            for column in record?.into_iter() {
                let field = Table::parse_field(column.to_string())?;
                fields.push(field);
            }
            rows.push(Row {
                fields
            })
        }
        Ok(rows)
    }

    fn parse_field(value: String) -> anyhow::Result<Field, anyhow::Error> {
        if value.chars().all(|char| char.is_digit(10)) {
            Ok(Field::Integer(value.parse()?))
        } else {
            Ok(Field::Text(value))
        }
    }

    fn parse_columns<R: std::io::Read>(reader: &mut csv::Reader<R>, rows: &Vec<Row>) -> Result<Vec<Column>, Error> {
        let headers: Vec<String> = reader.headers()?.into_iter().map(|header| header.to_string()).collect();
        let mut columns: Vec<Column> = Vec::new();
        for (index, header) in headers.into_iter().enumerate() {
            let mut column_values: Vec<&Field> = Vec::new();
            for row in rows.iter() {
                let row_field = row.fields.get(index).ok_or_else(|| anyhow!("Row {:?} does not have column {:?}", &row, &header))?;
                column_values.push(row_field);
            }
            let is_integer_column = column_values.into_iter().all(|field| match field {
                Field::Integer(_) => true,
                Field::Text(_) => false
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
    fn should_parse_table_from_csv() {
        let input = r#"column1,column2,column3
aaa,1,10
bbb,2,b
ccc,3,11"#;
        let mut reader = ReaderBuilder::new().from_reader(Cursor::new(input));
        let table = Table::from(&mut reader).unwrap();
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
                    fields: vec![Field::Text("aaa".to_string()), Field::Integer(1), Field::Integer(10)]
                },
                Row {
                    fields: vec![Field::Text("bbb".to_string()), Field::Integer(2), Field::Text("b".to_string())]
                },
                Row {
                    fields: vec![Field::Text("ccc".to_string()), Field::Integer(3), Field::Integer(11)]
                }
            ]
        })
    }
}