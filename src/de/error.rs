use std::fmt::{ Display, Formatter, Error as FmtError };
use std::error::Error;
use serde::de::{Error as SerdeError};

#[derive(Debug)]
pub struct AvroError {
    pub reason: String
}

impl SerdeError for AvroError {
    fn custom<T: Display>(input: T) -> Self {
        AvroError{
            reason: format!("serde sez {}", input)
        }
    }
}

impl Error for AvroError {

}

impl Display for AvroError {
    fn fmt(&self, fmt: &mut Formatter) -> Result<(), FmtError> {
        write!(fmt, "I got an error. Whoops!")
    }
}