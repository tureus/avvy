use serde::de::{DeserializeSeed, MapAccess};

use super::*;

pub struct AvroValueMapAccess<'a, 'de: 'a> {
    pub de: &'a mut AvroDeserializer<'de>,
    pub blocks: i64,
    pub entries: i64,
}

impl<'a, 'de> MapAccess<'de> for AvroValueMapAccess<'a, 'de> {
    type Error = AvroError;

    /// This returns `Ok(Some(key))` for the next key in the map, or `Ok(None)`
    /// if there are no more remaining entries.
    ///
    /// `Deserialize` implementations should typically use
    /// `MapAccess::next_key` or `MapAccess::next_entry` instead.
    fn next_key_seed<K>(&mut self, seed: K) -> Result<Option<K::Value>, Self::Error>
        where K: DeserializeSeed<'de> {
        info!("next_value_seed (entry {})", self.entries);
        if self.de.peek() == 0 {
            Ok(None)
        } else if self.entries <= 0 {
            Ok(None)
        } else {
            let val = seed.deserialize(&mut *self.de).map(Some);
            self.entries -= 1;
            val
        }
    }

    /// This returns a `Ok(value)` for the next value in the map.
    ///
    /// `Deserialize` implementations should typically use
    /// `MapAccess::next_value` instead.
    ///
    /// # Panics
    ///
    /// Calling `next_value_seed` before `next_key_seed` is incorrect and is
    /// allowed to panic or return bogus results.
    fn next_value_seed<V>(&mut self, seed: V) -> Result<V::Value, Self::Error>
        where
            V: DeserializeSeed<'de> {
        info!("next_value_seed");
        seed.deserialize(&mut *self.de)
    }

    /// Returns the number of entries remaining in the map, if known.
    #[inline]
    fn size_hint(&self) -> Option<usize> {
        Some(self.blocks as usize)
    }
}