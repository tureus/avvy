use serde::de::{ SeqAccess, DeserializeSeed };

use super::*;

pub struct AvroSeqVisitor<'a, 'de: 'a> {
    de: &'a mut AvroDeserializer<'de>,
    blocks: usize,
}

impl<'a, 'de> AvroSeqVisitor<'a, 'de> {
    pub fn new(de: &'a mut AvroDeserializer<'de>) -> Self {
        let mut blocks = de.visit_long();
        if blocks < 0 {
            blocks *= -1;
        }
        Self { de, blocks: blocks as usize }
    }
}

impl<'a, 'de> SeqAccess<'de> for AvroSeqVisitor<'a, 'de> {
    type Error = AvroError;

    fn next_element_seed<T>(&mut self, seed: T) -> Result<Option<T::Value>, Self::Error>
        where T: DeserializeSeed<'de> {
        if self.de.peek() == 0 {
            return Ok(None)
        }

        seed.deserialize(&mut *self.de).map(Some)
    }

    fn size_hint(&self) -> Option<usize> {
        Some(self.blocks)
    }
}