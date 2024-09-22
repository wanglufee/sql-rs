use std::iter::Peekable;

use ast::{Column, Expression, Statement};
use lexer::{Lexer, Token, Keyword};

use crate::error::{Result, Error};

use super::types::DataType;

mod lexer;
pub mod ast;

// 解析器，拿到词法分析的结果进行语法分析，最终生成抽象语法树。
pub struct Parser<'a> {
    lexer: Peekable<Lexer<'a>>,
}

impl<'a> Parser<'a> {
    pub fn new(input: &'a str) -> Self {
        Self{
            lexer: Lexer::new(input).peekable()
        }
    }

    // 解析
    fn parse(&mut self) -> Result<Statement> {
        let stmt = self.parse_statement()?;
        // 希望以分号结尾
        self.next_expect(Token::Semicolon)?;
        // 分号之后不再有内容
        if let Some(token) = self.peek()? {
            return Err(Error::Parse(format!("[Parser] Unexpected token {}", token)));
        }
        Ok(stmt)
    }

    fn parse_statement(&mut self) -> Result<Statement> {
        // 查看第一个字符
        match self.peek()? {
            Some(Token::Keyword(Keyword::Create)) => self.parse_ddl(),
            Some(Token::Keyword(Keyword::Select)) => self.parse_select(),
            Some(Token::Keyword(Keyword::Insert)) => self.parse_insert(),
            Some(t) => Err(Error::Parse(format!("[Parser] Unexpected token {}", t))),
            None => Err(Error::Parse(format!("[Parser] Unexpected end of input"))),
        }
    }

    // 解析ddl语句
    fn parse_ddl(&mut self) -> Result<Statement> {
        match self.next()? {
            // 期望是 Create 关键字
            Token::Keyword(Keyword::Create) => match self.next()? {
                // Create 关键字之后应该是 Table 关键字
                Token::Keyword(Keyword::Table) => self.parse_ddl_create_table(),
                token => Err(Error::Parse(format!("[Parser] Unexpected token {}", token))),
            },
            token => Err(Error::Parse(format!("[Parser] Unexpected token {}", token))),
        }
    }

    // 解析 Select 语句
    fn parse_select(&mut self) -> Result<Statement> {
        self.next_expect(Token::Keyword(Keyword::Select))?;
        self.next_expect(Token::Asterisk)?;
        self.next_expect(Token::Keyword(Keyword::From))?;
        let table_name = self.next_ident()?;
        Ok(Statement::Select { table_name })
    }


    fn parse_insert(&mut self) -> Result<Statement> {
        self.next_expect(Token::Keyword(Keyword::Insert))?;
        self.next_expect(Token::Keyword(Keyword::Into))?;
        // 表名
        let table_name = self.next_ident()?;
        let columns = if self.next_if_token(Token::OpenParen).is_some() {
            let mut column = Vec::new();
            loop{
                column.push(self.next_ident()?);
                match self.next()? {
                    Token::CloseParen => break,
                    Token::Comma => {},
                    token => {
                        return Err(Error::Parse(format!("[Parser] Unexpected token {}", token)));
                    }
                }
            }
            Some(column)
        } else {
            None
        };
        self.next_expect(Token::Keyword(Keyword::Values))?;
        // insert into tbl(a, b, c) values (1, 2, 3),(4, 5, 6);
        let mut values = Vec::new();
        loop{
            self.next_expect(Token::OpenParen)?;
            let mut value = Vec::new();
            loop {
                value.push(self.parse_expression()?);
                match self.next()? {
                    Token::CloseParen => break,
                    Token::Comma => {},
                    token => {
                        return Err(Error::Parse(format!("[Parser] Unexpected token {}", token)));
                    }
                }
            }
            values.push(value);
            if self.next_if_token(Token::Comma).is_none() {
                break;
            }
        }
        Ok(Statement::Insert { 
            table_name, 
            columns, 
            values })
    }

    // 解析 Crate 的 ddl 语句
    fn parse_ddl_create_table(&mut self) -> Result<Statement> {
        // 期望是表名
        let table_name = self.next_ident()?;

        // 表名之后是左括号
        self.next_expect(Token::OpenParen)?;

        // 解析列
        let mut columns = Vec::new();
        loop{
            columns.push(self.parse_ddl_column()?);
            if self.next_if_token(Token::Comma).is_none() {
                break;
            }
        }
        // 右括号
        self.next_expect(Token::CloseParen)?;
        Ok(Statement::CreateTable { 
            name: table_name, 
            columns: columns 
        })
    }

    // 解析列
    fn parse_ddl_column(&mut self) -> Result<Column> {
        let mut column = Column{
            name: self.next_ident()?,
            datetype: match self.next()? {
                Token::Keyword(Keyword::Bool) | Token::Keyword(Keyword::Boolean) => DataType::Boolean,
                Token::Keyword(Keyword::Int) | Token::Keyword(Keyword::Integer) => DataType::Integer,
                Token::Keyword(Keyword::Float) | Token::Keyword(Keyword::Double) => DataType::Float,
                Token::Keyword(Keyword::String) | Token::Keyword(Keyword::Text) | Token::Keyword(Keyword::Varchar) => DataType::String,
                token => return Err(Error::Parse(format!("[Parser] Unexpected token {}", token))),
            },
            nullable: None,
            default: None,
        };
        // 判断下一个是否是关键字
        while let Some(Token::Keyword(keyword)) = self.next_if_keyword() {
            match keyword {
                Keyword::Null => column.nullable = Some(true),
                Keyword::Not => {
                    self.next_expect(Token::Keyword(Keyword::Null))?;
                    column.nullable = Some(false)
                }
                Keyword::Default => column.default = Some(self.parse_expression()?),
                k => return Err(Error::Parse(format!("[Parser] Unexpected keyword {}", k))),
            }
        }

        Ok(column)
    }

    // 解析表达式
    fn parse_expression(&mut self) -> Result<Expression> {
        Ok(match self.next()? {
            Token::Number(n) => {
                if n.chars().all(|c| c.is_ascii_digit()) {
                    ast::Consts::Integer(n.parse()?).into()
                } else {
                    ast::Consts::Float(n.parse()?).into()
                }
            },
            Token::String(s) => ast::Consts::String(s).into(),
            Token::Keyword(Keyword::True) => ast::Consts::Boolean(true).into(),
            Token::Keyword(Keyword::False) => ast::Consts::Boolean(false).into(),
            Token::Keyword(Keyword::Null) => ast::Consts::Null.into(),
            t => {
                return Err(Error::Parse(format!(
                    "[Parser] Unexpected expression token {}",
                    t
                )))
            }
        })
    }

    fn peek(&mut self) -> Result<Option<Token>> {
        self.lexer.peek().cloned().transpose()
    }

    fn next(&mut self) -> Result<Token> {
        self.lexer.next().unwrap_or_else(|| Err(Error::Parse(format!("[Parser] Unexpected end of input"))))
    }

    fn next_ident(&mut self) -> Result<String> {
        match self.next()? {
            Token::Ident(ident) => Ok(ident),
            token => Err(Error::Parse(format!(
                "[Parser] Expected ident, got token {}",
                token
            )))
        }
    }

    fn next_expect(&mut self, expect: Token) -> Result<()> {
        let token = self.next()?;
        if token != expect {
            return Err(Error::Parse(format!(
                "[Parser] Expected token {}, got {}",
                expect, token
            )));
        }
        Ok(())
    }

    fn next_if<F: Fn(&Token) -> bool>(&mut self, predict: F) -> Option<Token> {
        self.peek().unwrap_or(None).filter(|t| predict(t))?;
        self.next().ok()
    }

    fn next_if_keyword(&mut self) -> Option<Token> {
        self.next_if(|t| matches!(t, Token::Keyword(_)))
    }

    fn next_if_token(&mut self,token: Token) -> Option<Token> {
        self.next_if(|t| t==&token)
    }
}

#[cfg(test)]
mod tests {
    use crate::{error::Result, sql::parser::ast};

    use super::Parser;

    #[test]
    fn test_parser_create_table() -> Result<()> {
        let sql1 = "
            create table tbl1 (
                a int default 100,
                b float not null,
                c varchar null,
                d bool default true
            );
        ";
        let stmt1 = Parser::new(sql1).parse()?;

        let sql2 = "
        create            table tbl1 (
            a int default     100,
            b float not null     ,
            c varchar      null,
            d       bool default        true
        );
        ";
        let stmt2 = Parser::new(sql2).parse()?;
        assert_eq!(stmt1, stmt2);

        let sql3 = "
            create            table tbl1 (
            a int default     100,
            b float not null     ,
            c varchar      null,
            d       bool default        true
        )
        ";

        let stmt3 = Parser::new(sql3).parse();
        assert!(stmt3.is_err());
        Ok(())
    }

    #[test]
    fn test_parser_insert() -> Result<()> {
        let sql1 = "insert into tbl1 values (1, 2, 3, 'a', true);";
        let stmt1 = Parser::new(sql1).parse()?;
        assert_eq!(
            stmt1,
            ast::Statement::Insert {
                table_name: "tbl1".to_string(),
                columns: None,
                values: vec![vec![
                    ast::Consts::Integer(1).into(),
                    ast::Consts::Integer(2).into(),
                    ast::Consts::Integer(3).into(),
                    ast::Consts::String("a".to_string()).into(),
                    ast::Consts::Boolean(true).into(),
                ]],
            }
        );

        let sql2 = "insert into tbl2 (c1, c2, c3) values (3, 'a', true),(4, 'b', false);";
        let stmt2 = Parser::new(sql2).parse()?;
        assert_eq!(
            stmt2,
            ast::Statement::Insert {
                table_name: "tbl2".to_string(),
                columns: Some(vec!["c1".to_string(), "c2".to_string(), "c3".to_string()]),
                values: vec![
                    vec![
                        ast::Consts::Integer(3).into(),
                        ast::Consts::String("a".to_string()).into(),
                        ast::Consts::Boolean(true).into(),
                    ],
                    vec![
                        ast::Consts::Integer(4).into(),
                        ast::Consts::String("b".to_string()).into(),
                        ast::Consts::Boolean(false).into(),
                    ],
                ],
            }
        );

        Ok(())
    }

    #[test]
    fn test_parser_select() -> Result<()> {
        let sql = "select * from tbl1;";
        let stmt = Parser::new(sql).parse()?;
        assert_eq!(
            stmt,
            ast::Statement::Select {
                table_name: "tbl1".to_string()
            }
        );
        Ok(())
    }
}