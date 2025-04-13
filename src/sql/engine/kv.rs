use serde::{Deserialize, Serialize};

use crate::{error::{Error, Result}, sql::{schema::Table, types::{Row, Value}}, storage::{self, engine::Engine as StorageEngein}};

use super::{Engine, Transaction};

// kv Engine 定义，是对存储引擎的 MVCC 的封装
pub struct KVEngine<E : StorageEngein>{
    pub kv : storage::mvcc::Mvcc<E>,
}

impl<E: StorageEngein> KVEngine<E>  {
    pub fn new(engine: E) -> Self{
        Self{
            kv: storage::mvcc::Mvcc::new(engine)
        }
    }
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
        Ok(())
    }

    fn rollback(&self) -> Result<()> {
        Ok(())
    }

    fn create_row(&mut self, table_name: String, row: Row) -> Result<()> {
        let table = self.must_get_table(table_name.clone())?;
        // 检查类型有效性
        for (i,col) in table.columns.iter().enumerate() {
            match row[i].datatype() {
                None if col.nullable => {},
                None => return Err(Error::Internel(format!("column {} cannot be null",col.name))),
                Some(dt) => {
                    if dt != col.datatype {
                        return Err(Error::Internel(format!("column {} type mismatched",col.name)));
                    }
                },
            }
        }

        // 存放数据
        // 暂时以第一列作为主键
        let id = Key::Row(table_name, row[0].clone());
        let value = bincode::serialize(&row)?;
        self.txn.set(bincode::serialize(&id)?, value)?;

        Ok(())
    }

    fn scan_table(&self, table_name: String) -> Result<Vec<Row>> {
        let perfix = KeyPerfix::Row(table_name.clone());
        let results = self.txn.scan_prefix(bincode::serialize(&perfix)?)?;

        let mut rows = Vec::new();
        for result in results {
            let row: Row = bincode::deserialize(&result.value)?;
            rows.push(row);
        }
        Ok(rows)
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
    Row(String,Value),
}

#[derive(Debug, Serialize, Deserialize)]
enum KeyPerfix {
    Table,
    Row(String),
}


mod tests {
    use crate::{error::Result, sql::engine::Engine, storage::memory::MemoryEngine};

    use super::KVEngine;

    #[test]
    #[ignore = "事务变化"]
    fn test_create_table() -> Result<()> {
        let kvengine = KVEngine::new(MemoryEngine::new());
        let mut s = kvengine.session()?;

        s.execute("create table t1 (a int, b text default 'vv', c integer default 100);")?;
        s.execute("insert into t1 values(1, 'a', 1);")?;
        s.execute("insert into t1 values(2, 'b');")?;
        s.execute("insert into t1(c, a) values(200, 3);")?;

        s.execute("select * from t1;")?;

        Ok(())
    }
}