use std::thread::current;

use anyhow::{anyhow, Context, Error, Result};
use crate::value::Value;

#[derive(Debug, PartialEq)]
pub struct Query {
    pub column_names: Vec<String>,
    pub filter: Option<Filter>
}

impl Query {
    pub fn parse(input: &str) -> Result<Query, Error> {
        let tokens: Vec<&str> = input.split_whitespace().collect();
        let (query, final_position) = Query::parse_query(&tokens, 0)?;
        if final_position == tokens.len() {
            Ok(query)
        } else {
            Err(anyhow!(format!("Unexpected suffix found in {:?} at position {}", tokens, final_position)))
        }
    }

    fn parse_query(tokens: &Vec<&str>, position: usize) -> Result<(Query, usize), Error> {
        let (column_names, position_after_projection) = Query::parse_projection(tokens, position)?;
        let (filter, position_after_filter) = Query::parse_filter(tokens, position_after_projection)?;
        Ok((Query {
            column_names,
            filter
        }, position_after_filter))
    }

    fn parse_projection(tokens: &Vec<&str>, position: usize) -> Result<(Vec<String>, usize), Error> {
        if let Some(&token) = tokens.get(position) {
            if token == "PROJECT" {
                let mut current_position = position + 1;
                let mut column_names: Vec<String> = Vec::new();
                let mut all_columns_read = false;
                while current_position < tokens.len() && !all_columns_read {
                    let current_token = tokens[current_position];
                    if current_token.ends_with(",") {
                        column_names.push(current_token[..(current_token.len() - 1)].to_string());
                        current_position = current_position + 1;
                    } else if current_token != "FILTER" {
                        column_names.push(current_token.to_string());
                        all_columns_read = true;
                        current_position = current_position + 1;
                    } else {
                        all_columns_read = true;
                    }
                }
                if column_names.is_empty() {
                    Err(anyhow!("Projection column list is empty"))
                } else {
                    Ok((column_names, current_position))
                }
            } else {
                Err(anyhow!(format!("Expected to find keyword PROJECT in {:?} at position {}", tokens, position)))
            }
        } else {
            Err(anyhow!(format!("Could not parse projection part in {:?} at position {}", tokens, position)))
        }
    }

    fn parse_filter(tokens: &Vec<&str>, position: usize) -> Result<(Option<Filter>, usize), Error> {
        if let Some(&token) = tokens.get(position) {
            if token == "FILTER" {
                let column = tokens.get(position + 1)
                    .ok_or_else(|| anyhow!("Could not find column in the filter in {:?} at position {}", tokens, &position + 1))?;
                let filter_type = FilterType::from(tokens.get(position + 2)
                    .ok_or_else(|| anyhow!("Could not find operator '>' or '=' in the filter in {:?} at position {}", tokens, &position))?)
                    .context(format!("Unknown filter operator in {:?} at position {}", tokens, &position + 2))?;
                let value_input = tokens.get(position + 3).map(|value| value.trim_matches('"'));
                let value = Value::parse_value(value_input
                    .ok_or_else(|| anyhow!("Could not find value to filter by in the filter in {:?} at position {}", tokens, &position + 3))?.to_string())?;
                Ok((Some(Filter {
                    column_name: column.to_string(),
                    filter_type,
                    value
                }), position + 4))
            } else {
                Err(anyhow!(format!("Expected to find keyword FILTER in {:?} at position {}", tokens, position)))
            }
        } else {
            Ok((None, position))
        }
    }
}

#[derive(Debug, PartialEq)]
pub struct Filter {
    pub column_name: String,
    pub value: Value,
    pub filter_type: FilterType
}

#[derive(Debug, PartialEq)]
pub enum FilterType {
    Greater,
    Equal
}

impl FilterType {
    fn from(input: &str) -> Result<FilterType, Error> {
        match input {
            ">" => Ok(FilterType::Greater),
            "=" => Ok(FilterType::Equal),
            _ => Err(anyhow!(format!("Unknown filter type {}", input)))
        }
    }
}

#[cfg(test)]
mod test {
    use super::*;

    #[test]
    fn should_parse_correct_query_with_projection_and_greater_filter() {
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

    #[test]
    fn should_parse_correct_query_with_projection_using_a_single_column_and_greater_filter() {
        let input = "PROJECT col1 FILTER col3 > \"value\"";
        let query = Query::parse(input).unwrap();
        assert_eq!(query, Query {
            column_names: vec!["col1".to_string()],
            filter: Some(Filter {
                column_name: "col3".to_string(),
                value: Value::Text("value".to_string()),
                filter_type: FilterType::Greater
            })
        })
    }

    #[test]
    fn should_parse_query_which_uses_multiple_blanks_between_words() {
        let input = "PROJECT   col1,   col2  FILTER     col3    >   \"value\"";
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

    #[test]
    fn should_parse_query_with_an_equality_filter_and_number() {
        let input = "PROJECT col1, col2 FILTER col3 = 42";
        let query = Query::parse(input).unwrap();
        assert_eq!(query, Query {
            column_names: vec!["col1".to_string(), "col2".to_string()],
            filter: Some(Filter {
                column_name: "col3".to_string(),
                value: Value::Integer(42),
                filter_type: FilterType::Equal
            })
        })
    }

    #[test]
    fn should_produce_error_when_projection_column_list_is_empty() {
        let input = "PROJECT FILTER col3 > \"value\"";
        let query = Query::parse(input);
        match query {
            Err(e) => assert_eq!(e.to_string(), "Projection column list is empty"),
            Ok(_) => panic!("Error expected"),
        }
    }

    #[test]
    fn should_produce_error_when_column_is_missing_from_the_filter() {
        let input = "PROJECT col1, col2 FILTER > \"value\"";
        let query = Query::parse(input);
        match query {
            Err(e) => assert_eq!(
                e.to_string(),
                "Unknown filter operator in [\"PROJECT\", \"col1,\", \"col2\", \"FILTER\", \">\", \"\\\"value\\\"\"] at position 5"
            ),
            Ok(_) => panic!("Error expected"),
        }
    }

    #[test]
    fn should_parse_query_with_no_filter() {
        let input = "PROJECT col1, col2";
        let query = Query::parse(input).unwrap();
        assert_eq!(query, Query {
            column_names: vec!["col1".to_string(), "col2".to_string()],
            filter: None
        })
    }

    #[test]
    fn should_produce_error_when_typo_in_project_keyword() {
        let input = "PROJECTION col1, col2 FILTER col3 > \"value\"";
        let query = Query::parse(input);
        match query {
            Err(e) => assert_eq!(
                e.to_string(),
                "Expected to find keyword PROJECT in [\"PROJECTION\", \"col1,\", \"col2\", \"FILTER\", \"col3\", \">\", \"\\\"value\\\"\"] at position 0"
            ),
            Ok(_) => panic!("Error expected"),
        }
    }

    #[test]
    fn should_produce_error_when_typo_in_filter_keyword() {
        let input = "PROJECT col1, col2 FILTRE col3 > \"value\"";
        let query = Query::parse(input);
        match query {
            Err(e) => assert_eq!(
                e.to_string(),
                "Expected to find keyword FILTER in [\"PROJECT\", \"col1,\", \"col2\", \"FILTRE\", \"col3\", \">\", \"\\\"value\\\"\"] at position 3"
            ),
            Ok(_) => panic!("Error expected"),
        }
    }

    #[test]
    fn should_produce_error_when_operator_is_missing_in_filter() {
        let input = "PROJECT col1, col2 FILTER col3 \"value\"";
        let query = Query::parse(input);
        match query {
            Err(e) => assert_eq!(
                e.to_string(),
                "Unknown filter operator in [\"PROJECT\", \"col1,\", \"col2\", \"FILTER\", \"col3\", \"\\\"value\\\"\"] at position 5"
            ),
            Ok(_) => panic!("Error expected"),
        }
    }

    #[test]
    fn should_produce_error_when_value_is_missing_in_filter() {
        let input = "PROJECT col1, col2 FILTER col3 >";
        let query = Query::parse(input);
        match query {
            Err(e) => assert_eq!(
                e.to_string(),
                "Could not find value to filter by in the filter in [\"PROJECT\", \"col1,\", \"col2\", \"FILTER\", \"col3\", \">\"] at position 6"
            ),
            Ok(_) => panic!("Error expected"),
        }
    }

    #[test]
    fn should_produce_error_when_there_are_dangling_symbols_after_query_left() {
        let input = "PROJECT col1, col2 FILTER col3 > \"value\". abc";
        let query = Query::parse(input);
        match query {
            Err(e) => assert_eq!(
                e.to_string(),
                "Unexpected suffix found in [\"PROJECT\", \"col1,\", \"col2\", \"FILTER\", \"col3\", \">\", \"\\\"value\\\".\", \"abc\"] at position 7"
            ),
            Ok(_) => panic!("Error expected"),
        }
    }
}
