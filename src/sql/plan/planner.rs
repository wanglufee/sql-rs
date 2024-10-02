use crate::sql::{parser::ast::Statement, schema::{Column, Table}, types::Value};

use super::{Node, Plan};



pub struct Planner;

impl Planner {
    pub fn new() -> Self {
        Self
    }

    pub fn build(&mut self, stm: Statement) -> Plan {
        Plan(self.build_statment(stm))
    }

    fn build_statment(&self, stm: Statement) -> Node {
        match stm {
            Statement::CreateTable { name, columns } => {
                Node::CreateTable { schema: Table{
                    name,
                    columns: columns.into_iter().map(|c| {
                        let nullable = c.nullable.unwrap_or(true);
                        let default = match c.default {
                            Some(expr) => {
                                Some(Value::from_expression(expr))
                            },
                            None if nullable => Some(Value::Null),
                            None => None,
                        };
                        Column {
                            name: c.name,
                            datatype: c.datatype,
                            nullable,
                            default,
                        }
                    }).collect(),
                } }
            },
            Statement::Insert { table_name, columns, values } => {
                Node::Insert { 
                    table_name, 
                    columns: columns.unwrap_or_default(), 
                    values 
                }
            },
            Statement::Select { table_name } => {
                Node::Scan { 
                    table_name 
                }
            },
        }
    }
}