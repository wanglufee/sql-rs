use std::{collections::HashSet, sync::{Arc, Mutex, MutexGuard}, u64};

use serde::{Deserialize, Serialize};

use crate::error::{Error, Result};

use super::engine::Engine;

pub type Version = u64;

pub struct Mvcc<E : Engine>{
    engine: Arc<Mutex<E>>,
}

impl<E : Engine> Clone for Mvcc<E> {
    fn clone(&self) -> Self {
        Self { engine: self.engine.clone() }
    }
}

impl<E : Engine> Mvcc<E> {
    pub fn new(eng: E) -> Self {
        Self { engine:Arc::new(Mutex::new(eng)) }
    }

    pub fn begin(&self) -> Result<MvccTransaction<E>> {
        MvccTransaction::begin(self.engine.clone())
    }
}


pub struct MvccTransaction<E : Engine> {
    engine: Arc<Mutex<E>>,
    state: TransactionState,
}

// 事务状态
pub struct TransactionState {
    // 当前版本号
    pub version: Version,
    // 当前活跃事务列表
    pub active_version: HashSet<Version>,
}

impl TransactionState {
    fn is_visible(&self, version: Version) -> bool {
        !self.active_version.contains(&version) && version < self.version
    }
}

#[derive(Debug, Serialize, Deserialize, PartialEq)]
pub enum MvccKey {
    NextVersion,
    TxnActive(Version),
    TxnWrite(
        Version, 
        #[serde(with = "serde_bytes")] 
        Vec<u8>
    ),
    Version(
        #[serde(with = "serde_bytes")] 
        Vec<u8>, 
        Version
    ),
}

impl MvccKey {
    pub fn encode(&self) -> Vec<u8> {
        bincode::serialize(self).unwrap()
    }

    pub fn decode(data: Vec<u8>) -> Result<Self> {
        Ok(bincode::deserialize(&data)?)
    }
}

#[derive(Debug, Serialize, Deserialize)]
pub enum MvccKeyPerfix {
    NextVersion,
    TxnActive,
    TxnWrite(Version),
    Version(
        #[serde(with = "serde_bytes")] 
        Vec<u8>
    ),
}

impl MvccKeyPerfix {
    pub fn encode(&self) -> Vec<u8> {
        bincode::serialize(self).unwrap()
    }
}

impl<E : Engine> MvccTransaction<E> {
    pub fn begin(eng: Arc<Mutex<E>>) -> Result<Self> {
        // 获取引擎
        let mut engine = eng.lock()?;
        // 获取版本号，第一次获取时给一个版本号默认值
        let next_version = match engine.get(MvccKey::NextVersion.encode())? {
            Some(value) => bincode::deserialize(&value)?,
            None => 1,
        };
        // 保存下一个version
        engine.set(MvccKey::NextVersion.encode(), bincode::serialize(&(next_version +1))?)?;
        // 获取当前活跃的事务列表
        let active_version = Self::scan_active(&mut engine)?;

        // 将当前事务加入活跃事务列表
        engine.set(MvccKey::TxnActive(next_version).encode(), vec![])?;

        Ok(Self{
            engine: eng.clone(),
            state: TransactionState{
                version: next_version,
                active_version: active_version,
            }
        })
    }

    // 提交事务
    pub fn commit(&self) -> Result<()> {
        let mut engine = self.engine.lock()?;

        // 拿到 TxnWrite 的信息，然后将其删掉
        let mut iter = engine.scan_prefix(MvccKeyPerfix::TxnWrite(self.state.version).encode());

        let mut delete_keys = Vec::new();
        while let Some((key,_)) = iter.next().transpose()? {
            delete_keys.push(key);
        }
        drop(iter);

        for key in delete_keys.into_iter() {
            engine.delete(key)?;
        }

        // 从活跃事务列表中删除当前版本
        engine.delete(MvccKey::TxnActive(self.state.version).encode())
    }

    // 回滚事务
    pub fn rollback(&self) -> Result<()> {
        let mut engine = self.engine.lock()?;

        // 拿到 TxnWrite 的信息，然后将其删掉
        let mut iter = engine.scan_prefix(MvccKeyPerfix::TxnWrite(self.state.version).encode());
        // 回滚时需要将修改的数据一并删除
        let mut delete_keys = Vec::new();
        while let Some((key,_)) = iter.next().transpose()? {
            match MvccKey::decode(key.clone())? {
                MvccKey::TxnWrite(_, key) => {
                    delete_keys.push(MvccKey::Version(key, self.state.version).encode());
                },
                _ => {
                    return Err(Error::Internel(format!("unexpect key: {:?}", String::from_utf8(key))));
                },
            }
            delete_keys.push(key);
        }
        drop(iter);

        for key in delete_keys.into_iter() {
            engine.delete(key)?;
        }

        // 从活跃事务列表中删除当前版本
        engine.delete(MvccKey::TxnActive(self.state.version).encode())
    }

    // 插入数据
    pub fn set(&self,key:Vec<u8>,value:Vec<u8>) -> Result<()> {
        self.write_inner(key, Some(value))
    }

    // 删除数据
    pub fn delete(&self,key:Vec<u8>) -> Result<()> {
        self.write_inner(key, None)
    }

    // 获取数据
    pub fn get(&self,key:Vec<u8>) -> Result<Option<Vec<u8>>> {
        let mut eng = self.engine.lock()?;
        // 从版本0到当前版本进行扫描，获取可见的最新版本
        let from = MvccKey::Version(key.clone(), 0).encode();
        let to = MvccKey::Version(key.clone(), self.state.version).encode();
        let mut iter = eng.scan(from..=to).rev();

        while let Some((key, value)) = iter.next().transpose()? {
            match MvccKey::decode(key.clone())? {
                MvccKey::Version(_, version) => {
                    if self.state.is_visible(version) {
                        return Ok(bincode::deserialize(&value)?);
                    }
                },
                _ => {
                    return Err(Error::Internel(format!("unexpect key: {:?}", String::from_utf8(key))));
                }
            }
        }

        Ok(None)
    }


    pub fn scan_prefix(&self,prefix: Vec<u8>) -> Result<Vec<ScanResult>> {
        let mut eng = self.engine.lock()?;
        let mut iter = eng.scan_prefix(prefix);
        let mut v = Vec::new();
        while let Some((key,value)) = iter.next().transpose()? {
            v.push(ScanResult{key,value});
        }
        Ok(v)
    }

    fn write_inner(&self, key: Vec<u8>, value: Option<Vec<u8>>) -> Result<()> {
        // 获取存储引擎
        let mut engine = self.engine.lock()?;

        // 检测冲突，获取当前活跃版本号中的最小值，开始扫描
        let from = MvccKey::Version(key.clone(), self.state.active_version.iter().min().copied().unwrap_or(self.state.version + 1)).encode();
        let to = MvccKey::Version(key.clone(), u64::MAX).encode();

        if let Some((key,_)) = engine.scan(from..=to).last().transpose()? {
            match MvccKey::decode(key.clone())? {
                MvccKey::Version(_, version) => {
                    if !self.state.is_visible(version) {
                        return Err(Error::WriteConflict)
                    }
                },
                _ => return Err(Error::Internel(format!("unexpect key: {:?}", String::from_utf8(key)))),
            }
        }

        // 记录这个 version 写入哪些 key，用于回滚事务
        engine.set(MvccKey::TxnWrite(self.state.version, key.clone()).encode(), vec![])?;

        // 写入实际的数据
        engine.set(MvccKey::Version(key.clone(), self.state.version).encode(), bincode::serialize(&value)?)?;

        Ok(())
    }

    // 扫描活跃事务
    fn scan_active(engine: &mut MutexGuard<E>) -> Result<HashSet<Version>> {
        let mut active_version = HashSet::new();
        let mut iter = engine.scan_prefix(MvccKeyPerfix::TxnActive.encode());
        while let Some((key,_)) = iter.next().transpose()? {
            match MvccKey::decode(key.clone())? {
                MvccKey::TxnActive(version) => {
                    active_version.insert(version);
                },
                _ => return Err(Error::Internel(format!("unexpect key: {:?}", String::from_utf8(key)))),
            }
        }
        Ok(active_version)
    }
}

#[derive(Debug)]
pub struct ScanResult {
    pub key: Vec<u8>,
    pub value: Vec<u8>,
}