use std::thread::current;

use anyhow::{anyhow, Context, Error, Result};
use crate::value::Value;

#[derive(Debug, PartialEq)]
struct Query {
    column_names: Vec<String>,
    filter: Option<Filter>
}

impl Query {
    pub(crate) fn parse(input: &str) -> Result<Query, Error> {
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
                let mut current_token = "";
                let mut current_position = position + 1;
                let mut column_names: Vec<String> = Vec::new();
                while current_position < tokens.len() && current_token != "FILTER" {
                    current_token = tokens[current_position];
                    if current_token != "FILTER" {
                        if current_token.ends_with(",") {
                            column_names.push(current_token[..(current_token.len() - 1)].to_string());
                        } else {
                            column_names.push(current_token.to_string())
                        }
                        current_position = current_position + 1;
                    }
                }
                Ok((column_names, current_position))
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
                    .ok_or_else(|| anyhow!("Could not find column in the filter in {:?} at position {}", tokens, &position))?;
                let filter_type = FilterType::from(tokens.get(position + 2)
                    .ok_or_else(|| anyhow!("Could not find operator '>' or '=' in the filter in {:?} at position {}", tokens, &position))?)
                    .context(format!("Unknown filter operator in {:?} at position {}", tokens, &position))?;
                let value_input = tokens.get(position + 3).map(|value| value.trim_matches('"'));
                let value = Value::parse_value(value_input
                    .ok_or_else(|| anyhow!("Could not find value to filter by in the filter in {:?} at position {}", tokens, &position))?.to_string())?;
                Ok((Some(Filter {
                    column_name: column.to_string(),
                    filter_type,
                    value
                }), position + 4))
            } else {
                Ok((None, position))
            }
        } else {
            Err(anyhow!(format!("Could not parse filter part in {:?} at position {}", tokens, position)))
        }
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

    //TODO: query which uses multiple blank symbols between words
    //TODO: = filter
    //TODO: only projection
    //TODO: syntax error, typo in PROJECT
    //TODO: syntax error, typo in FILTER
    //TODO: syntax error: empty list of columns
    //TODO: syntax error: > or = is missing in the FILTER
    //TODO: syntax error: value is missing after > or = in FILTER
    //TODO: syntax error: extra unrelated dangling symbols after the query ended
}
