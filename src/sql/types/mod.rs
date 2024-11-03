use serde::{Serialize,Deserialize};

use super::parser::ast::{Consts, Expression};

// 数据类型，目前只有基本类型
#[derive(Debug,Clone,Serialize,Deserialize, PartialEq)]
pub enum DataType {
    Boolean,
    Integer,
    Float,
    String,
}


#[derive(Debug, PartialEq, Serialize, Deserialize, Clone)]
pub enum Value {
    Null,
    Boolean(bool),
    Integer(i64),
    Float(f64),
    String(String),
}

impl Value {
    pub fn from_expression(expr: Expression) -> Self {
        match expr {
            Expression::Consts(Consts::Null) => Self::Null,
            Expression::Consts(Consts::Boolean(b)) => Self::Boolean(b),
            Expression::Consts(Consts::Float(f)) => Self::Float(f),
            Expression::Consts(Consts::Integer(i)) => Self::Integer(i),
            Expression::Consts(Consts::String(s)) => Self::String(s),
        }
    }

    pub fn datatype(&self) -> Option<DataType>{
        match self {
            Value::Null => None,
            Value::Boolean(_) => Some(DataType::Boolean),
            Value::Integer(_) => Some(DataType::Integer),
            Value::Float(_) => Some(DataType::Float),
            Value::String(_) => Some(DataType::String),
        }
    }
}

pub type Row = Vec<Value>;