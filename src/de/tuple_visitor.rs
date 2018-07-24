use serde::de::{ SeqAccess, DeserializeSeed };

use super::*;

pub struct AvroTupleVisitor<'a, 'de: 'a> {
    de: &'a mut AvroDeserializer<'de>,
    size: usize
}

impl<'a, 'de> AvroTupleVisitor<'a, 'de> {
    pub fn new(de: &'a mut AvroDeserializer<'de>, size: usize) -> Self {
        Self { de, size }
    }
}

impl<'a, 'de> SeqAccess<'de> for AvroTupleVisitor<'a, 'de> {
    type Error = AvroError;

    fn next_element_seed<T>(&mut self, seed: T) -> Result<Option<T::Value>, Self::Error>
        where T: DeserializeSeed<'de> {
        seed.deserialize(&mut *self.de).map(Some)
    }

    fn size_hint(&self) -> Option<usize> {
        Some(self.size)
    }
}