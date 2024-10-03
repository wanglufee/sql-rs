use crate::{error::Result, sql::schema::{self, Table}};

use super::Executor;

// 创建表
pub struct CreateTable {
    schema: Table,
}


impl CreateTable {
    pub fn new(schema: Table) -> Box<Self> {
        Box::new(Self{ schema })
    }
}

impl Executor for CreateTable {
    fn execute(&self) -> Result<super::ResultSet> {
        todo!()
    }
}