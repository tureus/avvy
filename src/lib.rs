#![feature(nll)]

#[macro_use] extern crate serde;
#[macro_use] extern crate serde_derive;

extern crate serde_json;

extern crate integer_encoding;
extern crate byteorder;

use serde::{Deserialize, Deserializer};
use serde::de::{ Visitor, EnumAccess, IntoDeserializer };

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

#[derive(Debug,Clone)]
enum AvroType {
    Primitive(AvroPrimitiveFields),
    StringMap,
    Fixed{name: String, size: usize}
}

#[derive(Debug,Clone)]
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
            }
        ]
    }
}

struct AvroDeserializer<'de> {
    buf: &'de [u8],
    visitor: AvroVisitor,
    current_field_index: usize
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

struct AvroMapVisitor<'a, 'de: 'a> {
    de: &'a mut AvroDeserializer<'de>,
    count: usize,
    expected: usize,
}

impl<'de, 'a> serde::de::MapAccess<'de> for AvroMapVisitor<'a, 'de> {
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
        let variant = self.de.visit_varint();
        println!("EnumAccess::variant_seed: {}", variant);

        let val = seed.deserialize((variant as u32).into_deserializer())?;

//        let val = match seed.deserialize(&mut *self.de) {
//            Ok(t) => t,
//            Err(e) => {
//                println!("error! {:#?}", e);
//                panic!("not sure how to direct deserialize");
//            }
//        };

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
        unimplemented!()
    }

    fn deserialize_i64<V>(mut self, visitor: V) -> Result<V::Value,Self::Error>
        where V: Visitor<'de> {
        visitor.visit_i64(self.visit_i64())
    }
    fn deserialize_i8<V>(mut self, visitor: V) -> Result<V::Value,Self::Error> where V: Visitor<'de> {
         unimplemented!()
    }

    fn deserialize_i16<V>(mut self, visitor: V) -> Result<V::Value,Self::Error> where V: Visitor<'de> {
         unimplemented!()
    }

    fn deserialize_i32<V>(mut self, visitor: V) -> Result<V::Value,Self::Error> where V: Visitor<'de> {
         unimplemented!()
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

        visitor.visit_map(AvroMapVisitor {de: &mut self, count: 0, expected: fields.len()})
    }

    fn deserialize_map<V>(mut self, visitor: V) -> Result<V::Value,Self::Error>
        where V: Visitor<'de> {
        unimplemented!()
    }

    fn deserialize_identifier<V>(mut self, visitor: V) -> Result<V::Value, Self::Error>
        where V: Visitor<'de> {
        println!("deserialize identifier...");
        let current_field = self.next_field();
        println!("deserialize_identifier {}", current_field.name);

        visitor.visit_string(current_field.name.clone())
    }

    fn deserialize_enum<V>(mut self, enum_name: &'static str, enum_variants: &[&'static str], visitor: V) -> Result<V::Value, Self::Error>
        where V: Visitor<'de> {
        let field = self.current_field();
        println!("deserialize_enum");

        match field.types {
            AvroTypeOneOrMany::One(_) => {
                unimplemented!()
            },
            AvroTypeOneOrMany::Many(ref _types) => {
                println!("visiting enum");
                let value = visitor.visit_enum(AvroEnumVisitor::new(self, enum_name, enum_variants) )?;
                Ok(value)
            }
        }
    }

    forward_to_deserialize_any!{
        <V: Visitor<'de>>
        bool char str string bytes byte_buf option unit unit_struct newtype_struct seq tuple tuple_struct ignored_any
    }
}

impl<'de> AvroDeserializer<'de> {
    fn dump(&self) {
        println!("{:#?}", self.buf);
    }

    fn skip(&mut self, bytes: usize) {
        self.buf = &self.buf[bytes..];
    }

    fn visit_i64(&mut self) -> i64 {
        use byteorder::{ ByteOrder, LittleEndian };
        let val = LittleEndian::read_i64(&self.buf[..8]);
        self.buf = &self.buf[8..];
        val
    }

    fn visit_u64(&mut self) -> u64 {
        use byteorder::{ ByteOrder, LittleEndian };
        let val = LittleEndian::read_u64(&self.buf[..8]);
        self.buf = &self.buf[7..];
        val
    }

    fn visit_varint(&mut self) -> i64 {
        let (int,varsize) : (i64, usize) = integer_encoding::VarInt::decode_var(self.buf);
        self.buf = &self.buf[varsize..];
        int
    }

    fn visit_long(&mut self) -> i64 {
        self.visit_varint()
    }

    fn visit_str(&mut self) -> &'de [u8] {
        let (strlen,strstart) : (u64, usize) = integer_encoding::VarInt::decode_var(&self.buf[..]);

        let rstr = &self.buf[strstart..strstart+strlen as usize];
        self.buf = &self.buf[strstart+strlen as usize..];

        rstr
    }

    fn visit_strmap(&mut self) -> Vec<(&[u8],&[u8])> {
        let num_blocks = self.visit_varint();
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
            current_field_index: 0,
        }
    }

    fn next_field(&mut self) -> &AvroField {
        println!("incr next_field: {}", self.current_field_index);
        let avro_field = &self.visitor.fields[self.current_field_index];
        self.current_field_index += 1;
        avro_field
    }

    fn current_field(&mut self) -> &AvroField {
        &self.visitor.fields[self.current_field_index]
    }

    fn done_with_filed(&mut self) {
        self.current_field_index += 1;
    }
}

#[derive(Deserialize,Debug)]
struct UT {
    timestamp: Timestamp,
    metric: String,
//    value: Value,
}

#[derive(Deserialize,Debug)]
enum Timestamp {
    Long(u64),
    Int(i64),
    Float(f32),
    Double(f64)
}

enum Value {
    Long8(u8),
    Long16(u16),
    Long32(u32),
    Long64(u64),
    I8(i8),
    I16(i16),
    I32(i32),
    Int64(i64),
    Float(f32),
    Double(f64)
}

#[test]
fn avro_deserializer() {
    let record : [u8; 257] = [0, 0, 0, 2, 106, 0, 186, 149, 235, 179, 11, 86, 118, 105, 97, 115, 97, 116, 45, 97, 98, 45, 118, 110, 111, 45, 112, 109, 46, 117, 116, 46, 112, 100, 102, 46, 102, 108, 45, 115, 100, 117, 45, 109, 97, 114, 107, 101, 100, 45, 99, 111, 117, 110, 116, 0, 0, 2, 22, 10, 97, 110, 45, 105, 100, 2, 49, 10, 112, 100, 102, 105, 100, 8, 49, 48, 53, 50, 16, 115, 109, 97, 99, 100, 45, 105, 100, 6, 49, 52, 55, 24, 115, 97, 116, 101, 108, 108, 105, 116, 101, 45, 105, 100, 2, 52, 34, 115, 109, 97, 99, 45, 115, 101, 114, 118, 105, 99, 101, 45, 110, 97, 109, 101, 26, 115, 109, 97, 99, 45, 99, 104, 105, 48, 55, 45, 115, 50, 16, 109, 97, 99, 45, 97, 100, 100, 114, 24, 48, 48, 97, 48, 98, 99, 56, 99, 55, 57, 55, 102, 10, 115, 116, 97, 116, 101, 14, 111, 110, 95, 108, 105, 110, 101, 14, 98, 101, 97, 109, 45, 105, 100, 10, 49, 49, 48, 52, 53, 22, 99, 97, 114, 114, 105, 101, 114, 100, 45, 105, 100, 2, 55, 12, 118, 110, 111, 45, 105, 100, 6, 120, 99, 105, 44, 115, 101, 114, 118, 105, 110, 103, 45, 115, 109, 97, 99, 45, 104, 111, 115, 116, 45, 110, 97, 109, 101, 36, 115, 109, 97, 99, 45, 99, 104, 105, 48, 55, 45, 110, 50, 45, 98, 101, 116, 97, 0, 0];

    let visitor = schema();
    let mut deserializer = AvroDeserializer::from_slice( visitor,&record[..]);
    deserializer.skip(3);

    let t = UT::deserialize(&mut deserializer);
    panic!("t: {:#?}", t);
}