use std::fmt::{Display, Formatter, Error as FmtError};
use std::error::Error;

use serde::de::{ Deserializer, Visitor, Error as SerdeError };

struct CDRDeserializer<'de> {
    pub buf: &'de [u8]
}

#[derive(Debug)]
struct CDRError {
    pub reason: String
}

impl Error for CDRError {

}

impl Display for CDRError {
    fn fmt(&self, fmt: &mut Formatter) -> Result<(), FmtError> {
        write!(fmt, "{}", self.reason)
    }
}

impl SerdeError for CDRError {
    fn custom<T: Display>(input: T) -> Self {
        CDRError{
            reason: format!("serde sez {}", input)
        }
    }
}

impl<'a, 'de> Deserializer<'de> for &'a mut CDRDeserializer<'de> {
    type Error = CDRError;

    fn deserialize_any<V>(self, _: V) -> Result<V::Value,Self::Error>
        where V: Visitor<'de> {
        unimplemented!("we don't do free form deserialization")
    }

    forward_to_deserialize_any! {
        <V: Visitor<'de>>
        bool i8 i16 i32 i64 u8 u16 u32 u64 f32 f64 char str string bytes byte_buf option unit unit_struct newtype_struct seq tuple tuple_struct map struct enum identifier ignored_any
    }
}

#[test]
fn hello() {
    let bytes = vec![0, 1, 2, 3];
    let _de = CDRDeserializer{
        buf: &bytes[..]
    };
}
