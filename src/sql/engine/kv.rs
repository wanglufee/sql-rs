use serde::{Deserialize, Serialize};

use crate::{error::{Error, Result}, sql::{schema::Table, types::Row}, storage::{self, engine::Engine as StorageEngein}};

use super::{Engine, Transaction};

// kv Engine 定义，是对存储引擎的 MVCC 的封装
pub struct KVEngine<E : StorageEngein>{
    pub kv : storage::mvcc::Mvcc<E>,
}

impl<E : StorageEngein> Clone for KVEngine<E> {
    fn clone(&self) -> Self {
        Self { kv: self.kv.clone() }
    }
}

impl<E : StorageEngein> Engine for KVEngine<E> {
    type Transaction = KVTransaction<E>;

    fn begin(&self) -> Result<Self::Transaction> {
        Ok(Self::Transaction::new(self.kv.begin()?))
    }
}

// KV Transaction 定义，实际是对存储引擎 MVCCTransaction 的封装
pub struct KVTransaction<E : StorageEngein> {
    txn: storage::mvcc::MvccTransaction<E>,
}

impl<E : StorageEngein> KVTransaction<E> {
    pub fn new(txn : storage::mvcc::MvccTransaction<E>) -> Self {
        Self { 
            txn 
        }
    }
}

impl<E : StorageEngein> Transaction for KVTransaction<E> {
    fn commit(&self) -> Result<()> {
        todo!()
    }

    fn rollback(&self) -> Result<()> {
        todo!()
    }

    fn create_row(&mut self, table: String, row: Row) -> Result<()> {
        todo!()
    }

    fn scan_table(&self, table_name: String) -> Result<Row> {
        todo!()
    }

    // 创建表，此处去调用底层存储引擎的接口
    fn create_table(&mut self, table: Table) -> Result<()> {
        // 判断表是否已经存在
        if self.get_table(table.name.clone())?.is_some() {
            return Err(Error::Internel(format!("table {} already exists",table.name)));
        }
        // 判断表的有效性
        if table.columns.is_empty() {
            return Err(Error::Internel(format!("table {} has no columns",table.name)));
        }
        // 将表名序列化作为键，将整张表序列化作为值
        let key = Key::Table(table.name.clone());
        let value = bincode::serialize(&table)?;
        self.txn.set(bincode::serialize(&key)?, value)?;
        Ok(())
    }

    fn get_table(&self, table_name: String) -> Result<Option<Table>> {
        let key = Key::Table(table_name);
        Ok(self.txn.get(bincode::serialize(&key)?)?
                .map(|v| bincode::deserialize(&v))
                .transpose()?)
    }
}

#[derive(Debug, Serialize, Deserialize)]
enum Key {
    Table(String),
    Row(String,String),
}