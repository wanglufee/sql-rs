
use crate::error::{Error, Result};

use super::{executor::ResultSet, parser::Parser, plan::Plan, schema::Table, types::Row};

mod kv;

pub trait Engine : Clone {
    type Transaction: Transaction;

    // 开启事务
    fn begin(&self) -> Result<Self::Transaction>;

    fn session(&self) -> Result<Session<Self>> {
        Ok(Session{
            engine: self.clone(),
        })
    }
}


// 抽象的事务信息，包含了 DML，DDL 操作
// 底层可以接入普通的 KV 引擎，也可以接入分布式存储引擎
pub trait Transaction {
    // 提交事物
    fn commit(&self) -> Result<()>;

    // 回滚事物
    fn rollback(&self) -> Result<()>;

    // 创建行
    fn create_row(&mut self, table_name: String, row: Row) -> Result<()>;

    // 扫描表
    fn scan_table(&self, table_name: String) -> Result<Vec<Row>>;

    // DDL相关操作
    fn create_table(&mut self, table: Table) -> Result<()>;

    // 获取表信息
    fn get_table(&self, table_name: String) -> Result<Option<Table>>;

    // 必须拿到表名
    fn must_get_table(&self, table_name: String) -> Result<Table> {
        self.get_table(table_name.clone())?.ok_or(Error::Internel(
            format!("table {} dose not exist!", table_name)
        ))
    }
}

// 客户端 session 定义
pub struct Session<E: Engine> {
    engine: E,
}

impl<E: Engine> Session<E> {
    
    // 执行客户端 sql 语句
    pub fn execute(&mut self, sql: &str) -> Result<ResultSet> {
        match Parser::new(sql).parse()? {
            stmt => {
                // 开启一个事务
                let mut txn = self.engine.begin()?;

                match Plan::build(stmt).execute(&mut txn) {
                    Ok(result) => {
                        // 执行成功，提交事务
                        txn.commit()?;
                        Ok(result)
                    },
                    Err(err) => {
                        // 执行失败，回滚事务
                        txn.rollback()?;
                        Err(err)
                    }
                }
            }
        }
    }
}