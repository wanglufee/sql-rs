use std::sync::PoisonError;

use bincode::ErrorKind;



pub type Result<T> = std::result::Result<T,Error>;

#[derive(Debug, Clone, PartialEq)]
pub enum Error {
    Parse(String),
    Internel(String),
}

impl From<std::num::ParseIntError> for Error {
    fn from(value: std::num::ParseIntError) -> Self {
        Error::Parse(value.to_string())
    }
}

impl From<std::num::ParseFloatError> for Error {
    fn from(value: std::num::ParseFloatError) -> Self {
        Error::Parse(value.to_string())
    }
}

impl<T> From<PoisonError<T>> for Error {
    fn from(value: PoisonError<T>) -> Self {
        Error::Internel(value.to_string())
    }
}

impl From<Box<ErrorKind>> for Error {
    fn from(value: Box<ErrorKind>) -> Self {
        Error::Internel(value.to_string())
    }
}