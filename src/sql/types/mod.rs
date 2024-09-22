use serde::{Serialize,Deserialize};

// 数据类型，目前只有基本类型
#[derive(Debug,Clone,Serialize,Deserialize, PartialEq)]
pub enum DataType {
    Boolean,
    Integer,
    Float,
    String,
}