use crate::sql::types::DataType;

// 抽象语法树的定义
#[derive(Debug,PartialEq)]
pub enum Statement{
    CreateTable {
        name: String,
        columns: Vec<Column>,
    },
    Insert {
        table_name: String,
        columns: Option<Vec<String>>,
        values: Vec<Vec<Expression>>,
    },
    Select {
        table_name: String,
    },
}

// 列定义
#[derive(Debug,PartialEq)]
pub struct Column {
    pub name: String,
    pub datetype: DataType,
    pub nullable: Option<bool>,
    pub default: Option<Expression>,
}


// 表达式定义
#[derive(Debug,PartialEq)]
pub enum Expression {
    Consts(Consts),
}


impl From<Consts> for Expression {
    fn from(value: Consts) -> Self {
        Self::Consts(value)
    }
}


// 常量定义
#[derive(Debug,PartialEq)]
pub enum Consts {
    Null,
    Boolean(bool),
    Integer(i64),
    Float(f64),
    String(String),
}