use std::collections::HashMap;

use crate::{error::{Error, Result}, sql::{engine::Transaction, parser::ast::Expression, schema::Table, types::{Row, Value}}};

use super::{Executor, ResultSet};

// 插入数据
pub struct Insert {
    table_name: String,
    columns: Vec<String>,
    values: Vec<Vec<Expression>>,
}

impl Insert {
    pub fn new(table_name: String,
        columns: Vec<String>,
        values: Vec<Vec<Expression>>) -> Box<Self> {
            Box::new(Self{
                table_name,
                columns,
                values,
            })
        }
}

// 对列进行对齐
// insert into tab values(1,2,3);
// 列有   a         b           c          d
// 值有   1         2           3
// 那么需要给 d 列进行对齐
fn pad_row(table: &Table, row: &Row) -> Result<Row> {
    let mut result = row.clone();
    // 跳过以指定值的部分
    for column in table.columns.iter().skip(row.len()) {
        if let Some(default) = column.default.clone() {
            result.push(default);
        } else {
            return Err(Error::Internel(format!("No default value for column {}!",column.name)));
        }
    }
    Ok(result)
}

// 对列进行对齐
// insert into tab(d,c) values(2,3);
// 列有   a         b           c          d
// 值有 default   default       2          3
fn make_row(table: &Table, column: &Vec<String>, row: &Row) -> Result<Row> {
    // 现判断指定的列和给定的值个数是否匹配
    if column.len() != row.len() {
        return Err(Error::Internel(format!("columns and values num mismatch")));
    }
    // 构造 hashmap 来保存制定的列和值
    let mut input = HashMap::new();
    for (i,col) in column.iter().enumerate() {
        input.insert(col, row[i].clone());
    }

    let mut result = Vec::new();
    for col in table.columns.iter() {
        if let Some(value) = input.get(&col.name) {
            result.push(value.clone());
        } else if let Some(value) = col.default.clone() {
            result.push(value);
        } else {
            return Err(Error::Internel(format!("No value given for the column {}",col.name)));
        }
    }
    Ok(result)
}


impl<T: Transaction> Executor<T> for Insert {
    fn execute(self: Box<Self>, txn: &mut T) -> Result<ResultSet> {
        // 插入值时现取出表信息
        let table = txn.must_get_table(self.table_name.clone())?;
        let mut count = 0;
        // 将表达式转换为值类型
        for exprs in self.values {
            let row = exprs.into_iter().map(|e| Value::from_expression(e)).collect::<Vec<_>>();
            // 如果未指定列值
            let insert_row = if self.columns.is_empty() {
                pad_row(&table, &row)?
            } else {
                // 制定了插入的列
                make_row(&table, &self.columns, &row)?
            };

            txn.create_row(self.table_name.clone(), insert_row)?;
            count += 1;
        }

        Ok(ResultSet::Insert { count })

    }
}