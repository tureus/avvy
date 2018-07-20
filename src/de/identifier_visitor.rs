use serde::de::{MapAccess, DeserializeSeed};
use super::*;

pub struct AvroIdentifierMapVisitor<'a, 'de: 'a> {
    pub de: &'a mut AvroDeserializer<'de>,
    pub count: usize,
    pub expected: usize,
}

impl<'de, 'a> MapAccess<'de> for AvroIdentifierMapVisitor<'a, 'de> {
    type Error = AvroError;

    fn next_key_seed<K>(&mut self, seed: K) -> Result<Option<K::Value>, Self::Error>
        where
            K: DeserializeSeed<'de> {
        info!("next_key_seed");
        if self.count >= self.expected {
            Ok(None)
        } else {
            self.count += 1;
            seed.deserialize(&mut *self.de).map(Some)
        }
    }

    fn next_value_seed<V>(&mut self, seed: V) -> Result<V::Value, Self::Error>
        where
            V: DeserializeSeed<'de> {
        info!("next_value_seed");
        seed.deserialize(&mut *self.de)
    }
}