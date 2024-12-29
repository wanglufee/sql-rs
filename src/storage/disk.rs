use std::{collections::BTreeMap, io::{BufWriter, Read, Seek, SeekFrom, Write}};
use serde::de::value;

use crate::error::Result;

type KeyDir = BTreeMap<Vec<u8>, (u64,u32)>;
const LOG_HEAD_SIZE:u32 = 8;

// 磁盘存储引擎
pub struct DiskEngine{
    keydir: KeyDir,
    log: Log,
}

impl super::engine::Engine for DiskEngine {
    type EngineIterator<'a> = DiskEngineIterator;

    fn set(&mut self, key: Vec<u8>, value: Vec<u8>) -> Result<()> {
        // 先写日志
        let (offset,size) = self.log.write_entry(&key, Some(&value))?;
        // 更新内存索引
        // 100----------------|-----150
        //                   130
        // val size = 20
        let val_size = value.len() as u32;
        // 条目中存入 value 在文件中的偏移以及 value 的长度
        self.keydir.insert(key, (offset + size as u64 - val_size as u64, val_size));
        Ok(())
    }

    fn get(&mut self, key: Vec<u8>) -> Result<Option<Vec<u8>>> {
        match self.keydir.get(&key) {
            Some((offset,val_size)) => {
                let val = self.log.read_value(*offset, *val_size)?;
                Ok(Some(val))
            },
            None => Ok(None)
        }
    }

    fn delete(&mut self, key: Vec<u8>) -> Result<()> {
        // 删除则写入None 并且从 keydir 中删除key条目
        self.log.write_entry(&key, None)?;
        self.keydir.remove(&key);
        Ok(())
    }

    fn scan(&mut self, range: impl std::ops::RangeBounds<Vec<u8>>) -> Self::EngineIterator<'_> {
        todo!()
    }
}


pub struct DiskEngineIterator {

}

impl super::engine::EngineIterator for DiskEngineIterator {
    
}

impl Iterator for DiskEngineIterator {
    type Item = Result<(Vec<u8>,Vec<u8>)>;

    fn next(&mut self) -> Option<Self::Item> {
        todo!()
    }
}

impl DoubleEndedIterator for DiskEngineIterator {
    fn next_back(&mut self) -> Option<Self::Item> {
        todo!()
    }
}


pub struct Log {
    file: std::fs::File
}

impl Log {
    
    fn write_entry(&mut self,key: &Vec<u8>, value: Option<&Vec<u8>>) -> Result<(u64,u32)> {
        // 定位到文件末尾
        let offset = self.file.seek(SeekFrom::End(0))?;
        // 计算长度
        let key_size = key.len() as u32;
        let val_size = value.map_or(0, |v| v.len() as u32);
        let total_size = key_size + val_size + LOG_HEAD_SIZE;
        // 拿到写入缓存
        let mut writer = BufWriter::with_capacity(total_size as usize, &self.file);
        writer.write_all(&key_size.to_be_bytes())?;
        writer.write_all(&value.map_or(-1, |v| v.len() as i32).to_be_bytes())?;
        writer.write_all(&key)?;
        if let Some(v) = value {
            writer.write_all(&v)?;
        }
        writer.flush();
        // 返回相对应文件的偏移，和写入的总长度。
        Ok((offset, total_size))
    }

    fn read_value(&mut self,offset: u64, val_size: u32) -> Result<Vec<u8>> {
        // 定位到 value 所在位置
        self.file.seek(SeekFrom::Start(offset))?;
        // 定义存储 value 的 buf
        let mut buf = vec![0;val_size as usize];
        self.file.read_exact(&mut buf)?;
        Ok(buf)
    }
}