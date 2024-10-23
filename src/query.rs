use anyhow::{anyhow, Result, Error};
use crate::table::Value;

#[derive(Debug, PartialEq)]
struct Query {
    column_names: Vec<String>,
    filter: Option<Filter>
}

impl Query {
    pub(crate) fn parse(input: &str) -> Result<Query, Error> {
        Ok(Query {
            column_names: Vec::new(),
            filter: None
        })
    }
}

#[derive(Debug, PartialEq)]
struct Filter {
    column_name: String,
    value: Value,
    filter_type: FilterType
}

#[derive(Debug, PartialEq)]
enum FilterType {
    Greater,
    Equal
}

#[cfg(test)]
mod test {
    use super::*;

    #[test]
    fn should_parse_correct_query_with_projection_and_filter() {
        let input = "PROJECT col1, col2 FILTER col3 > \"value\"";
        let query = Query::parse(input).unwrap();
        assert_eq!(query, Query {
            column_names: vec!["col1".to_string(), "col2".to_string()],
            filter: Some(Filter {
                column_name: "col3".to_string(),
                value: Value::Text("value".to_string()),
                filter_type: FilterType::Greater
            })
        })
    }

    //TODO: = filter
    //TODO: only projection
    //TODO: syntax error, typo in PROJECT
    //TODO: syntax error, typo in FILTER
    //TODO: syntax error: empty list of columns
    //TODO: syntax error: > or = is missing in the FILTER
    //TODO: syntax error: value is missing after > or = in FILTER
}
