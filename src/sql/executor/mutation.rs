use crate::{error::Result, sql::parser::ast::Expression};

use super::Executor;

// 插入数据
pub struct Insert {
    table_name: String,
    columns: Vec<String>,
    values: Vec<Vec<Expression>>,
}

impl Insert {
    pub fn new(table_name: String,
        columns: Vec<String>,
        values: Vec<Vec<Expression>>) -> Box<Self> {
            Box::new(Self{
                table_name,
                columns,
                values,
            })
        }
}


impl Executor for Insert {
    fn execute(&self) -> Result<super::ResultSet> {
        todo!()
    }
}