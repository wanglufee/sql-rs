use mutation::Insert;
use query::Scan;
use schema::CreateTable;

use crate::error::Result;

use super::{plan::Node, types::Row};


mod schema;
mod mutation;
mod query;

// 执行其trait
pub trait Executor {
    fn execute(&self) -> Result<ResultSet>;
}

impl dyn Executor {
    // 根据执行计划节点生成对应执行器
    pub fn build(node: Node) -> Box<dyn Executor> {
        match node {
            Node::CreateTable { schema } => CreateTable::new(schema),
            Node::Insert { table_name, columns, values } => Insert::new(table_name, columns, values),
            Node::Scan { table_name } => Scan::new(table_name),
        }
    }
}

// 执行结果定义
pub enum ResultSet {
    CreateTable {
        table_name: String,
    },
    Insert {
        count: usize,
    },
    Scan {
        columns: Vec<String>,
        row: Vec<Row>
    }
}