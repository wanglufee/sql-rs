use crate::{error::Result, storage};

use super::{Engine, Transaction};

// kv Engine 定义，是对存储引擎的 MVCC 的封装
pub struct KVEngine{
    pub kv : storage::Mvcc,
}

impl Clone for KVEngine {
    fn clone(&self) -> Self {
        Self { kv: self.kv.clone() }
    }
}

impl Engine for KVEngine {
    type Transaction = KVTransaction;

    fn begin(&self) -> Result<Self::Transaction> {
        Ok(Self::Transaction::new(self.kv.begin()?))
    }
}

// KV Transaction 定义，实际是对存储引擎 MVCCTransaction 的封装
pub struct KVTransaction {
    txn: storage::MvccTransaction,
}

impl KVTransaction {
    pub fn new(txn : storage::MvccTransaction) -> Self {
        Self { 
            txn 
        }
    }
}

impl Transaction for KVTransaction {
    fn commit(&self) -> Result<()> {
        todo!()
    }

    fn rollback(&self) -> Result<()> {
        todo!()
    }

    fn create_row(&mut self, table: String, row: crate::sql::types::Row) -> Result<()> {
        todo!()
    }

    fn scan_table(&self, table_name: String) -> Result<crate::sql::types::Row> {
        todo!()
    }

    fn create_table(&mut self, table: crate::sql::schema::Table) -> Result<()> {
        todo!()
    }

    fn get_table(&self, table_name: String) -> Result<Option<crate::sql::schema::Table>> {
        todo!()
    }
}