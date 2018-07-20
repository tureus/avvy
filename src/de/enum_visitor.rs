use serde::de::{ EnumAccess, DeserializeSeed, Visitor, VariantAccess, IntoDeserializer };

use super::*;

pub struct AvroEnumVisitor<'a, 'de: 'a> {
    de: &'a mut AvroDeserializer<'de>,
}

impl<'a, 'de> AvroEnumVisitor<'a, 'de> {
    pub fn new(de: &'a mut AvroDeserializer<'de>, enum_name: &'static str, enum_variants: &'a[&'static str]) -> Self {
        Self { de }
    }
}

// `EnumAccess` is provided to the `Visitor` to give it the ability to determine
// which variant of the enum is supposed to be deserialized.
//
// Note that all enum deserialization methods in Serde refer exclusively to the
// "externally tagged" enum representation.
impl<'de, 'a> EnumAccess<'de> for AvroEnumVisitor<'a, 'de> {
    type Error = AvroError;
    type Variant = Self;

    fn variant_seed<V>(self, seed: V) -> Result<(V::Value, Self::Variant), Self::Error>
        where
            V: DeserializeSeed<'de>,
    {
        // This is the index in to the timestamp enum
        let variant = self.de.visit_uint();
        info!("EnumAccess::variant_seed: {}", variant);

        let val = seed.deserialize((variant as u32).into_deserializer())?;

        Ok((val,self))
    }
}

// `VariantAccess` is provided to the `Visitor` to give it the ability to see
// the content of the single variant that it decided to deserialize.
impl<'de, 'a> VariantAccess<'de> for AvroEnumVisitor<'a, 'de> {
    type Error = AvroError;

    // If the `Visitor` expected this variant to be a unit variant, the input
    // should have been the plain string case handled in `deserialize_enum`.
    fn unit_variant(self) -> Result<(),Self::Error> {
        panic!("unit_variant never called")
    }

    fn newtype_variant_seed<T>(self, seed: T) -> Result<T::Value, AvroError>
        where
            T: DeserializeSeed<'de>,
    {
        seed.deserialize(self.de)
    }

    fn tuple_variant<V>(self, _len: usize, _visitor: V) -> Result<V::Value, AvroError>
        where
            V: Visitor<'de>,
    {
        panic!("tuple_variant never called")
    }

    fn struct_variant<V>(
        self,
        _fields: &'static [&'static str],
        _visitor: V,
    ) -> Result<V::Value, AvroError>
        where
            V: Visitor<'de>,
    {
        panic!("struct_variant never called")
//        de::Deserializer::deserialize_map(self.de, visitor)
    }
}