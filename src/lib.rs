#![feature(nll)]

#[macro_use] extern crate serde;
#[macro_use] extern crate serde_derive;

extern crate serde_json;

extern crate integer_encoding;
extern crate byteorder;

use serde::{Deserialize, Deserializer};
use serde::de::{ Visitor, EnumAccess, IntoDeserializer, MapAccess };

#[derive(Serialize, Deserialize, Debug)]
struct Schema {
    #[serde(rename = "type")]
    schema_type: String,
    name: String,
    namespace: String,
    fields: Vec<SchemaField>,
}

impl Schema {
    fn from_str(schema: &str) -> serde_json::Result<Self> {
        serde_json::from_str(schema)
    }
}

#[derive(Serialize, Deserialize, Debug)]
struct SchemaField {
    name: String,
    #[serde(rename = "type", deserialize_with = "one_or_many")]
    types: Vec<SchemaFieldType>,
}

#[derive(Serialize, Deserialize, Debug)]
#[serde(untagged)]
enum SchemaFieldType {
    Primitive(Primitive),
    Complex(Complex),
}

#[derive(Serialize, Deserialize, Debug)]
#[serde(rename_all = "lowercase")]
enum Primitive {
    Null,
    Int,
    Long,
    Float,
    Double,
    Boolean,
    Bytes,
    String,
    #[serde(rename = "uint64_t")]
    Uint64T,
    #[serde(rename = "int64_t")]
    Int64T,
}

#[derive(Serialize, Deserialize, Debug)]
#[serde(tag = "type", rename_all = "lowercase")]
enum Complex {
    Fixed {
        name: String,
        size: usize,
    },
    Map {
        values: String,
    },
}

fn one_or_many<'de, D>(deserializer: D) -> Result<Vec<SchemaFieldType>, D::Error>
    where
        D: Deserializer<'de>,
{
    #[derive(Deserialize)]
    #[serde(untagged)]
    enum OneOrMany {
        One(SchemaFieldType),
        Many(Vec<SchemaFieldType>),
    }

    match OneOrMany::deserialize(deserializer)? {
        OneOrMany::One(field) => Ok(vec![field]),
        OneOrMany::Many(fields) => Ok(fields),
    }
}

const SCHEMA_STR: &'static str = r###"{
      "type": "record",
      "name": "ut",
      "namespace": "vnoportal",
      "fields": [
        {
          "name": "timestamp",
          "type": [
            "long",
            "int",
            "float",
            "double",
            {
              "type": "fixed",
              "name": "uint64_t",
              "size": 8
            },
            {
              "type": "fixed",
              "name": "int64_t",
              "size": 8
            }
          ]
        },
        {
          "name": "metric",
          "type": "string"
        },
        {
          "name": "value",
          "type": [
            "long",
            "int",
            "float",
            "double",
            {
              "type": "fixed",
              "name": "uint8_t",
              "size": 1
            },
            {
              "type": "fixed",
              "name": "uint16_t",
              "size": 2
            },
            {
              "type": "fixed",
              "name": "uint32_t",
              "size": 4
            },
            "uint64_t",
            {
              "type": "fixed",
              "name": "int8_t",
              "size": 1
            },
            {
              "type": "fixed",
              "name": "int16_t",
              "size": 2
            },
            {
              "type": "fixed",
              "name": "int32_t",
              "size": 4
            },
            "int64_t"
          ]
        },
        {
          "name": "tags",
          "type": [
            "null",
            {
              "type": "map",
              "values": "string"
            }
          ]
        },
        {
          "name": "metadata",
          "type": [
            "null",
            {
              "type": "map",
              "values": "string"
            }
          ]
        }
      ]
    }"###;

struct AvroVisitor {
    fields: Vec<AvroField>
}

#[derive(Debug,Clone)]
struct AvroField {
    name: String,
    types: AvroTypeOneOrMany
}

#[derive(Debug,Clone)]
enum AvroTypeOneOrMany {
    One(AvroType),
    Many(Vec<AvroType>)
}

#[derive(Debug,Clone,PartialEq)]
enum AvroType {
    Primitive(AvroPrimitiveFields),
    StringMap,
    Fixed{name: String, size: usize}
}

#[derive(Debug,Clone,PartialEq)]
enum AvroPrimitiveFields {
    Null,
    Int,
    Long,
    Float,
    Double,
    Boolean,
    Bytes,
    String
}

fn schema() -> AvroVisitor {
    AvroVisitor {
        fields: vec![
            AvroField {
                name: "timestamp".into(),
                types: AvroTypeOneOrMany::Many(
                    vec![
                        AvroType::Primitive(AvroPrimitiveFields::Long),
                        AvroType::Primitive(AvroPrimitiveFields::Int),
                        AvroType::Primitive(AvroPrimitiveFields::Float),
                        AvroType::Primitive(AvroPrimitiveFields::Double),
                        AvroType::Fixed{name: "uint64_t".into(), size: 8},
                        AvroType::Fixed{name: "int64_t".into(), size: 8}
                    ]
                )
            },
            AvroField {
                name: "metric".into(),
                types: AvroTypeOneOrMany::One(
                    AvroType::Primitive(AvroPrimitiveFields::String)
                )
            },
            AvroField {
                name: "value".into(),
                types: AvroTypeOneOrMany::Many(
                    vec![
                        AvroType::Primitive(AvroPrimitiveFields::Long),
                        AvroType::Primitive(AvroPrimitiveFields::Int),
                        AvroType::Primitive(AvroPrimitiveFields::Float),
                        AvroType::Primitive(AvroPrimitiveFields::Double),
                        AvroType::Fixed{name: "uint8_t".into(), size: 1},
                        AvroType::Fixed{name: "uint16_t".into(), size: 2},
                        AvroType::Fixed{name: "uint32_t".into(), size: 4},
                        AvroType::Fixed{name: "uint64_t".into(), size: 8},
                        AvroType::Fixed{name: "int8_t".into(), size: 1},
                        AvroType::Fixed{name: "int16_t".into(), size: 2},
                        AvroType::Fixed{name: "int32_t".into(), size: 4},
                        AvroType::Fixed{name: "int64_t".into(), size: 8},
                    ]
                )
            },
            AvroField {
                name: "tags".into(),
                types: AvroTypeOneOrMany::Many(
                    vec![
                        AvroType::Primitive(AvroPrimitiveFields::Null),
                        AvroType::StringMap
                    ]
                )
            },
            AvroField {
                name: "metadata".into(),
                types: AvroTypeOneOrMany::Many(
                    vec![
                        AvroType::Primitive(AvroPrimitiveFields::Null),
                        AvroType::StringMap
                    ]
                )
            }
        ]
    }
}

struct AvroDeserializer<'de> {
    buf: &'de [u8],
    visitor: AvroVisitor,
    current_field_index: Option<usize>
}

#[derive(Debug)]
struct AvroError {
    reason: String
}

impl std::fmt::Display for AvroError {
    fn fmt(&self, fmt: &mut std::fmt::Formatter) -> Result<(),std::fmt::Error> {
        write!(fmt, "I got an error. Whoops!")
    }
}

impl serde::de::Error for AvroError {
    fn custom<T: std::fmt::Display>(input: T) -> Self {
        AvroError{
            reason: format!("serde sez {}", input)
        }
    }
}

impl std::error::Error for AvroError {

}

struct AvroIdentifierMapVisitor<'a, 'de: 'a> {
    de: &'a mut AvroDeserializer<'de>,
    count: usize,
    expected: usize,
}

impl<'de, 'a> serde::de::MapAccess<'de> for AvroIdentifierMapVisitor<'a, 'de> {
    type Error = AvroError;

    fn next_key_seed<K>(&mut self, seed: K) -> Result<Option<K::Value>, Self::Error>
        where
            K: serde::de::DeserializeSeed<'de> {
        println!("next_key_seed");
        if self.count >= self.expected {
            Ok(None)
        } else {
            self.count += 1;
            seed.deserialize(&mut *self.de).map(Some)
        }
    }

    fn next_value_seed<V>(&mut self, seed: V) -> Result<V::Value, Self::Error>
        where
            V: serde::de::DeserializeSeed<'de> {
        println!("next_value_seed");
        seed.deserialize(&mut *self.de)
    }
}

struct AvroValueMapAccess<'a, 'de: 'a> {
    de: &'a mut AvroDeserializer<'de>,
    size: i64,
}

impl<'a, 'de> MapAccess<'de> for AvroValueMapAccess<'a, 'de> {
    type Error = AvroError;

    /// This returns `Ok(Some(key))` for the next key in the map, or `Ok(None)`
    /// if there are no more remaining entries.
    ///
    /// `Deserialize` implementations should typically use
    /// `MapAccess::next_key` or `MapAccess::next_entry` instead.
    fn next_key_seed<K>(&mut self, seed: K) -> Result<Option<K::Value>, Self::Error>
        where K: serde::de::DeserializeSeed<'de> {
        println!("next_value_seed (entry {})", self.size);
        if self.size == 0 {
            Ok(None)
        } else {
            let val = seed.deserialize(&mut *self.de).map(Some);
            self.size -= 1;
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
            V: serde::de::DeserializeSeed<'de> {
        println!("next_value_seed");
        seed.deserialize(&mut *self.de)
    }

    /// Returns the number of entries remaining in the map, if known.
    #[inline]
    fn size_hint(&self) -> Option<usize> {
        Some(self.size as usize)
    }
}

struct AvroEnumVisitor<'a, 'de: 'a> {
    de: &'a mut AvroDeserializer<'de>,
    enum_name: &'static str,
    enum_variants: &'a [&'static str],
    is_inside_enum: bool,
}

impl<'a, 'de> AvroEnumVisitor<'a, 'de> {
    fn new(de: &'a mut AvroDeserializer<'de>, enum_name: &'static str, enum_variants: &'a[&'static str]) -> Self {
        Self { de, enum_name, enum_variants, is_inside_enum: false }
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
            V: serde::de::DeserializeSeed<'de>,
    {
        // This is the index in to the timestamp enum
        let variant = self.de.visit_uint();
        println!("EnumAccess::variant_seed: {}", variant);

        let val = seed.deserialize((variant as u32).into_deserializer())?;

        Ok((val,self))
    }
}

// `VariantAccess` is provided to the `Visitor` to give it the ability to see
// the content of the single variant that it decided to deserialize.
impl<'de, 'a> serde::de::VariantAccess<'de> for AvroEnumVisitor<'a, 'de> {
    type Error = AvroError;

    // If the `Visitor` expected this variant to be a unit variant, the input
    // should have been the plain string case handled in `deserialize_enum`.
    fn unit_variant(self) -> Result<(),Self::Error> {
        panic!("unit_variant never called")
    }

    fn newtype_variant_seed<T>(self, seed: T) -> Result<T::Value, AvroError>
        where
            T: serde::de::DeserializeSeed<'de>,
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


impl<'de, 'a> Deserializer<'de> for &'a mut AvroDeserializer<'de> {
    type Error = AvroError;

    fn deserialize_any<V>(mut self, visitor: V) -> Result<V::Value,Self::Error>
        where V: Visitor<'de> {
        unimplemented!("we don't do free form deserialization")
    }

    fn deserialize_i64<V>(mut self, visitor: V) -> Result<V::Value,Self::Error>
        where V: Visitor<'de> {
        println!("deserialize_i64");
        visitor.visit_i64(self.visit_i64())
    }

    fn deserialize_i8<V>(mut self, visitor: V) -> Result<V::Value,Self::Error> where V: Visitor<'de> {
         unimplemented!()
    }

    fn deserialize_i16<V>(mut self, visitor: V) -> Result<V::Value,Self::Error> where V: Visitor<'de> {
         unimplemented!()
    }

    fn deserialize_i32<V>(mut self, visitor: V) -> Result<V::Value,Self::Error> where V: Visitor<'de> {
         println!("deserialize_i32");
        visitor.visit_i32(self.visit_i32())
    }

    fn deserialize_u8<V>(mut self, visitor: V) -> Result<V::Value,Self::Error> where V: Visitor<'de> {
         unimplemented!()
    }

    fn deserialize_u16<V>(mut self, visitor: V) -> Result<V::Value,Self::Error> where V: Visitor<'de> {
         unimplemented!()
    }

    fn deserialize_u32<V>(mut self, visitor: V) -> Result<V::Value,Self::Error> where V: Visitor<'de> {
         unimplemented!()
    }

    fn deserialize_u64<V>(mut self, visitor: V) -> Result<V::Value,Self::Error> where V: Visitor<'de> {
         unimplemented!()
    }

    fn deserialize_f32<V>(mut self, visitor: V) -> Result<V::Value,Self::Error> where V: Visitor<'de> {
         unimplemented!()
    }

    fn deserialize_f64<V>(mut self, visitor: V) -> Result<V::Value,Self::Error> where V: Visitor<'de> {
        unimplemented!()
    }


    fn deserialize_struct<V>(mut self, _id: &'static str, fields: &'static[&'static str], visitor: V) -> Result<V::Value,Self::Error>
        where V: Visitor<'de> {
        println!("deserialize_struct");

        visitor.visit_map(AvroIdentifierMapVisitor {de: &mut self, count: 0, expected: fields.len()})
    }

    fn deserialize_map<V>(mut self, visitor: V) -> Result<V::Value,Self::Error>
        where V: Visitor<'de> {
        let size = {
            self.visit_long()
        };
        let map_visitor = AvroValueMapAccess{de: &mut self, size};
        visitor.visit_map( map_visitor)
    }

    fn deserialize_identifier<V>(mut self, visitor: V) -> Result<V::Value, Self::Error>
        where V: Visitor<'de> {
        self.next_field();
        let current_field = self.current_field();
        println!("deserialize_identifier {}", current_field.name);

        let res = visitor.visit_string(current_field.name.clone());
        res
    }

    fn deserialize_enum<V>(mut self, enum_name: &'static str, enum_variants: &[&'static str], visitor: V) -> Result<V::Value, Self::Error>
        where V: Visitor<'de> {
        let field = self.current_field();
        println!("deserialize_enum enum_name: {}, enum_variants: {:?}", enum_name, enum_variants);

        match field.types {
            AvroTypeOneOrMany::One(ref thing) => {
                println!("enum is one of: {:?}", thing);
                let value = visitor.visit_enum(AvroEnumVisitor::new(self, enum_name, enum_variants) )?;
                Ok(value)
            },
            AvroTypeOneOrMany::Many(ref types) => {
                println!("enum is any of {:?}", types);
                let value = visitor.visit_enum(AvroEnumVisitor::new(self, enum_name, enum_variants) )?;
                Ok(value)
            }
        }
    }

    fn deserialize_string<V>(mut self, visitor: V) -> Result<V::Value, Self::Error>
        where V: Visitor<'de> {
        println!("deserialize string...");
        let string = String::from_utf8(self.visit_str().to_owned()).unwrap();
        visitor.visit_string(string)
    }

    fn deserialize_option<V>(mut self, visitor: V) -> Result<V::Value, Self::Error>
        where V: Visitor<'de> {
        println!("deserialize option...");
//        let string = String::from_utf8(self.visit_str().to_owned()).unwrap();
        let enum_variant = {
            self.visit_int() as usize
        };

        println!("option variant: {}", enum_variant);

        let current_field = self.current_field();
        match current_field.types {
            AvroTypeOneOrMany::Many(ref types) => {
                if enum_variant >= types.len() {
                    return Err(AvroError{reason: "option variant id out of scope".into()})
                } else if types[enum_variant] == AvroType::Primitive(AvroPrimitiveFields::Null) {
                    visitor.visit_none()
                } else {
                    visitor.visit_some(self)
                }
            },
            AvroTypeOneOrMany::One(_) => {
                return Err(AvroError{ reason: "this should be an option but it's a union in the schema!".into() })
            }
        }
    }

    forward_to_deserialize_any!{
        <V: Visitor<'de>>
        bool char str bytes byte_buf unit unit_struct newtype_struct seq tuple tuple_struct ignored_any
    }
}

impl<'de> AvroDeserializer<'de> {
    fn dump(&self) {
        println!("dumping: {:#?}", self.buf);
    }

    fn skip(&mut self, bytes: usize) {
        self.buf = &self.buf[bytes..];
    }

    fn visit_i32(&mut self) -> i32 {
        use byteorder::{ ByteOrder, LittleEndian };
        let val = LittleEndian::read_i32(&self.buf[..4]);
        println!("LittleEndian byte stuff: {}", val);

        let (val2,size2) : (i32,usize) = integer_encoding::VarInt::decode_var(self.buf);
        println!("val2: {}, size2: {}", val2, size2);

        self.buf = &self.buf[7..];
        val
    }

    fn visit_i64(&mut self) -> i64 {
        let (val,varsize) : (i64,usize) = integer_encoding::VarInt::decode_var(self.buf);
        println!("visit_i64 val2: {}, varsize: {}", val, varsize);

        self.buf = &self.buf[varsize..];
        val
    }

    fn visit_u64(&mut self) -> u64 {
        use byteorder::{ ByteOrder, LittleEndian };
        let val = LittleEndian::read_u64(&self.buf[..8]);
        self.buf = &self.buf[7..];
        val
    }

    fn visit_uint(&mut self) -> u32 {
        let (int,varsize) = integer_encoding::VarInt::decode_var(self.buf);
        self.buf = &self.buf[varsize..];
        int
    }

    fn visit_int(&mut self) -> i32 {
        let (int,varsize) = integer_encoding::VarInt::decode_var(self.buf);
        self.buf = &self.buf[varsize..];
        int
    }

    fn visit_long(&mut self) -> i64 {
        let (int,varsize) = integer_encoding::VarInt::decode_var(self.buf);
        self.buf = &self.buf[varsize..];
        int
    }

    fn visit_str(&mut self) -> &'de [u8] {
        let strlen = self.visit_long();
        println!("strlen: {}", strlen);

        let rstr = &self.buf[..strlen as usize];
        println!("rstr: {}", String::from_utf8(rstr.to_owned()).unwrap());

        self.buf = &self.buf[strlen as usize..];

        rstr
    }

    fn visit_strmap(&mut self) -> Vec<(&[u8],&[u8])> {
        let num_blocks = self.visit_long();
        println!("num_blocks: {}", num_blocks);

        let mut vec : Vec<(&[u8], &[u8])> = Vec::with_capacity(num_blocks as usize);

        for _i in 0..num_blocks {
            let key = self.visit_str();
            let val = self.visit_str();

            vec.push((key,val));
        }

        vec
    }

}

impl<'de> AvroDeserializer<'de> {
    fn from_slice(visitor: AvroVisitor, buf: &'de [u8]) -> Self {
        AvroDeserializer {
            buf,
            visitor,
            current_field_index: None,
        }
    }

    fn next_field(&mut self) {
        if let Some(ref mut cfi) = self.current_field_index {
            *cfi += 1;
        } else {
            self.current_field_index = Some(0);
        };
        println!("done with field, now on current_field_index {:?}", self.current_field_index);
    }

    fn current_field(&mut self) -> &AvroField {
        println!("current_field index: {:?}", self.current_field_index);
        &self.visitor.fields[self.current_field_index.unwrap()]
    }
}

#[derive(Deserialize,Debug)]
struct UT {
    timestamp: Timestamp,
    metric: String,
    value: Value,
    tags: Option<std::collections::HashMap<String,String>>,
    metadata: Option<std::collections::HashMap<String,String>>
}

#[derive(Deserialize,Debug)]
enum Timestamp {
    Long(i64),
    Int(i32),
    Float(f32),
    Double(f64)
}

#[derive(Deserialize,Debug)]
enum Value {
    Long(i64),
    Int(i32),
    Float(f32),
    Double(f64),
    Long8(u8),
    Long16(u16),
    Long32(u32),
    Long64(u64),
    I8(i8),
    I16(i16),
    I32(i32),
    Int64(i64)
}

#[test]
fn avro_deserializer() {
    let record : [u8; 257] = [0, 0, 0, 2, 106, 0, 186, 149, 235, 179, 11, 86, 118, 105, 97, 115, 97, 116, 45, 97, 98, 45, 118, 110, 111, 45, 112, 109, 46, 117, 116, 46, 112, 100, 102, 46, 102, 108, 45, 115, 100, 117, 45, 109, 97, 114, 107, 101, 100, 45, 99, 111, 117, 110, 116, 0, 0, 2, 22, 10, 97, 110, 45, 105, 100, 2, 49, 10, 112, 100, 102, 105, 100, 8, 49, 48, 53, 50, 16, 115, 109, 97, 99, 100, 45, 105, 100, 6, 49, 52, 55, 24, 115, 97, 116, 101, 108, 108, 105, 116, 101, 45, 105, 100, 2, 52, 34, 115, 109, 97, 99, 45, 115, 101, 114, 118, 105, 99, 101, 45, 110, 97, 109, 101, 26, 115, 109, 97, 99, 45, 99, 104, 105, 48, 55, 45, 115, 50, 16, 109, 97, 99, 45, 97, 100, 100, 114, 24, 48, 48, 97, 48, 98, 99, 56, 99, 55, 57, 55, 102, 10, 115, 116, 97, 116, 101, 14, 111, 110, 95, 108, 105, 110, 101, 14, 98, 101, 97, 109, 45, 105, 100, 10, 49, 49, 48, 52, 53, 22, 99, 97, 114, 114, 105, 101, 114, 100, 45, 105, 100, 2, 55, 12, 118, 110, 111, 45, 105, 100, 6, 120, 99, 105, 44, 115, 101, 114, 118, 105, 110, 103, 45, 115, 109, 97, 99, 45, 104, 111, 115, 116, 45, 110, 97, 109, 101, 36, 115, 109, 97, 99, 45, 99, 104, 105, 48, 55, 45, 110, 50, 45, 98, 101, 116, 97, 0, 0];

    let visitor = schema();
    let mut deserializer = AvroDeserializer::from_slice( visitor,&record[..]);
    deserializer.skip(5);

    let t = UT::deserialize(&mut deserializer);
    deserializer.dump();
    panic!("t: {:#?}", t);
}