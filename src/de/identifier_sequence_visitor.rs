use serde::de::{ SeqAccess, DeserializeSeed };

use super::*;

pub struct AvroIdentiferSeqVisitor<'a, 'de: 'a> {
    de: &'a mut AvroDeserializer<'de>,
    index: usize
}

impl<'a, 'de> AvroIdentiferSeqVisitor<'a, 'de> {
    pub fn new(de: &'a mut AvroDeserializer<'de>) -> Self {
        Self { de, index: 0 }
    }
}

impl<'a, 'de> SeqAccess<'de> for AvroSeqVisitor<'a, 'de> {
    type Error = AvroError;

    fn next_element_seed<T>(&mut self, seed: T) -> Result<Option<T::Value>, Self::Error>
        where T: DeserializeSeed<'de> {
        self.de.next_field();
        if self.de.has_more_fields() {
            Ok(Some(&self.de.current_field().name))
        } else {

        }

    }

    fn size_hint(&self) -> Option<usize> {
        Some(self.blocks)
    }
}