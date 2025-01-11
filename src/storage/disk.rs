use std::{collections::{btree_map, BTreeMap}, fs::{File, OpenOptions}, io::{BufReader, BufWriter, Read, Seek, SeekFrom, Write}, path::PathBuf};
use fs4::FileExt;

use crate::error::Result;

type KeyDir = BTreeMap<Vec<u8>, (u64,u32)>;
const LOG_HEAD_SIZE:u32 = 8;

// 磁盘存储引擎
pub struct DiskEngine{
    keydir: KeyDir,
    log: Log,
}

impl DiskEngine {
    pub fn new(file_path: PathBuf) -> Result<Self> {
        let mut log = Log::new(file_path)?;
        let keydir = log.build_keydir()?;
        Ok(Self { keydir, log })
    }


    pub fn new_compact(file_path: PathBuf) -> Result<Self> {
        let mut eng = Self::new(file_path)?;
        eng.compact()?;
        Ok(eng)
    }

    fn compact(&mut self) -> Result<()> {
        // 新建一个临时文件
        let mut new_path = self.log.file_path.clone();
        new_path.set_extension("compact");
        let mut new_log = Log::new(new_path)?;
        // 新建一个内存目录
        let mut new_keydir = KeyDir::new();
        // 遍历原目录并读取对应文件，生成新文件和目录
        for (key,(offset,val_size)) in self.keydir.iter() {
            let val = self.log.read_value(*offset, *val_size)?;
            let (offset, size) =new_log.write_entry(key, Some(&val))?;
            new_keydir.insert(key.clone(), (
                offset + size as u64 - *val_size as u64, *val_size  
            ));
        }
        // 将临时文件更名
        std::fs::rename(&new_log.file_path, &self.log.file_path)?;
        new_log.file_path = self.log.file_path.clone();
        // 将新的文件和目录替换调原来的
        self.log = new_log;
        self.keydir = new_keydir;
        
        Ok(())
    }
}

impl super::engine::Engine for DiskEngine {
    type EngineIterator<'a> = DiskEngineIterator<'a>;

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
        DiskEngineIterator{
            inner: self.keydir.range(range),
            log: &mut self.log,
        }
    }
}


pub struct DiskEngineIterator<'a> {
    inner: btree_map::Range<'a, Vec<u8>,(u64,u32)>,
    log: &'a mut Log,
}

impl<'a> DiskEngineIterator<'a> {
    fn map(&mut self,item: (&Vec<u8>, &(u64,u32))) -> <Self as Iterator>::Item {
        let (k,(offset,val_size)) = item;
        let value = self.log.read_value(*offset, *val_size)?;
        Ok((k.clone(),value))
    }
}

impl<'a> super::engine::EngineIterator for DiskEngineIterator<'a> {
    
}

impl<'a> Iterator for DiskEngineIterator<'a> {
    type Item = Result<(Vec<u8>,Vec<u8>)>;

    fn next(&mut self) -> Option<Self::Item> {
        self.inner.next().map(|item| self.map(item))
    }
}

impl<'a> DoubleEndedIterator for DiskEngineIterator<'a> {
    fn next_back(&mut self) -> Option<Self::Item> {
        self.inner.next_back().map(|item| self.map(item))
    }
}


pub struct Log {
    file_path: PathBuf,
    file: std::fs::File
}

impl Log {

    fn new(file_path: PathBuf) -> Result<Self> {
        // 文件夹不存在，创建文件夹
        if let Some(dir) = file_path.parent() {
            if !dir.exists() {
                std::fs::create_dir_all(&dir)?;
            }
        }
        // 打开文件
        let file = OpenOptions::new().create(true).read(true).write(true).open(&file_path)?;

        file.try_lock_exclusive()?;
        Ok(Self { file_path ,file })
    }

    fn build_keydir(&mut self) -> Result<KeyDir>{
        // 从文件构建内存目录
        let mut key_dir = KeyDir::new();
        let mut bufreader = BufReader::new(&self.file);
        let file_size = self.file.metadata()?.len();
        // 从文件头开始
        let mut offset: u64 = 0;
        loop {
            // 如果到文件末尾，退出
            if offset >= file_size {
                break;
            }
            // 读取条目
            let (key,val_size) = Self::read_entry(&mut bufreader, offset)?;
            let key_size = key.len();
            // 如果val_size为-1则说明被删除
            if val_size == -1 {
                key_dir.remove(&key);
                offset += key_size as u64 + LOG_HEAD_SIZE as u64;
            } else {
                key_dir.insert(key, (
                    offset + LOG_HEAD_SIZE as u64 + key_size as u64 , val_size as u32
                ));
                offset += key_size as u64 + LOG_HEAD_SIZE as u64 + val_size as u64;
            }
        }
        Ok(key_dir)
    }
    
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
        writer.flush()?;
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

    fn read_entry(bufreader: &mut BufReader<&File>, offset: u64) -> Result<(Vec<u8>,i32)>{
        bufreader.seek(SeekFrom::Start(offset))?;
        let mut len_buf = [0;4];

        // 读取 key 长度
        bufreader.read_exact(&mut len_buf)?;
        let key_size = u32::from_be_bytes(len_buf);

        // 读取 val 长度
        bufreader.read_exact(&mut len_buf)?;
        let val_size = i32::from_be_bytes(len_buf);

        // 读取 key
        let mut key = vec![0;key_size as usize];
        bufreader.read_exact(&mut key)?;

        Ok((key, val_size))
    }
}

#[cfg(test)]
mod test{
    use std::path::PathBuf;
    use crate::{error::Result, storage::engine::Engine};
    use super::DiskEngine;

    #[test]
    fn test_disk_engine_start() -> Result<()> {
        let _ = DiskEngine::new(PathBuf::from("/tmp/sqldp"))?;
        Ok(())
    }

    #[test]
    fn test_disk_engine_compact_start() -> Result<()> {
        let _ = DiskEngine::new_compact(PathBuf::from("/tmp/sqldp"))?;
        Ok(())
    }

    #[test]
    fn test_disk_engine_compact() -> Result<()> {
        let mut eng = DiskEngine::new(PathBuf::from("/tmp/sqldb/sqldb-log"))?;
        // 写一些数据
        eng.set(b"key1".to_vec(), b"value".to_vec())?;
        eng.set(b"key2".to_vec(), b"value".to_vec())?;
        eng.set(b"key3".to_vec(), b"value".to_vec())?;
        eng.delete(b"key1".to_vec())?;
        eng.delete(b"key2".to_vec())?;

        // 重写
        eng.set(b"aa".to_vec(), b"value1".to_vec())?;
        eng.set(b"aa".to_vec(), b"value2".to_vec())?;
        eng.set(b"aa".to_vec(), b"value3".to_vec())?;
        eng.set(b"bb".to_vec(), b"value4".to_vec())?;
        eng.set(b"bb".to_vec(), b"value5".to_vec())?;

        let iter = eng.scan(..);
        let v = iter.collect::<Result<Vec<_>>>()?;
        assert_eq!(
            v,
            vec![
                (b"aa".to_vec(), b"value3".to_vec()),
                (b"bb".to_vec(), b"value5".to_vec()),
                (b"key3".to_vec(), b"value".to_vec()),
            ]
        );
        drop(eng);

        let mut eng2 = DiskEngine::new_compact(PathBuf::from("/tmp/sqldb/sqldb-log"))?;
        let iter2 = eng2.scan(..);
        let v2 = iter2.collect::<Result<Vec<_>>>()?;
        assert_eq!(
            v2,
            vec![
                (b"aa".to_vec(), b"value3".to_vec()),
                (b"bb".to_vec(), b"value5".to_vec()),
                (b"key3".to_vec(), b"value".to_vec()),
            ]
        );
        drop(eng2);

        std::fs::remove_dir_all("/tmp/sqldb")?;

        Ok(())
    }
}