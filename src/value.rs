use std::cmp::Ordering;

#[derive(Debug, PartialEq, Eq, Clone)]
pub(crate) enum Value {
    Integer(u64),
    Text(String)
}

impl Ord for Value {
    fn cmp(&self, other: &Self) -> Ordering {
        match (self, other) {
            (Value::Integer(x), Value::Integer(y)) => x.cmp(y),
            (Value::Text(x), Value::Text(y)) => x.cmp(y),
            (x, y) => format!("{:?}", x).cmp(&format!("{:?}", y))
        }
    }
}

impl PartialOrd for Value {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl Value {
    pub(crate) fn parse_value(value: String) -> anyhow::Result<Value, anyhow::Error> {
        if value.chars().all(|char| char.is_digit(10)) {
            Ok(Value::Integer(value.parse()?))
        } else {
            Ok(Value::Text(value))
        }
    }
}
