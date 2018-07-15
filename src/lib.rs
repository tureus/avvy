#![feature(nll)]

#[macro_use] extern crate serde;
#[macro_use] extern crate serde_derive;

extern crate serde_json;

//extern crate integer_encoding;
extern crate byteorder;

use serde::{Deserialize, Deserializer};
use serde::de::{ Visitor, EnumAccess };
//use integer_encoding::{VarInt, VarIntReader, FixedInt, FixedIntReader};

// Copied out of the integer_encoding library
const DROP_MSB: u8 = 0b01111111;
pub const MSB: u8 = 0b10000000;
fn decode_var(src: &[u8]) -> (u64, usize) {
    let mut result: u64 = 0;
    let mut shift = 0;

    for b in src.iter() {
        let msb_dropped = b & DROP_MSB;
        result |= (msb_dropped as u64) << shift;
        shift += 7;

        if b & MSB == 0 || shift > (10 * 7) {
            break;
        }
    }

    (result, shift / 7 as usize)
}

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


//fn blow_up(){
//    let s = Schema::from_str(SCHEMA_STR).unwrap();
//
////    let record1 : [u8; 242] = [0, 0, 0, 2, 106, 0, 252, 136, 235, 179, 11, 94, 118, 105, 97, 115, 97, 116, 45, 97, 98, 45, 118, 110, 111, 45, 112, 109, 46, 117, 116, 46, 112, 100, 102, 46, 114, 108, 45, 115, 121, 109, 98, 111, 108, 115, 45, 103, 114, 97, 110, 116, 101, 100, 45, 114, 97, 116, 101, 6, 0, 0, 0, 0, 0, 0, 0, 0, 2, 20, 10, 97, 110, 45, 105, 100, 2, 49, 10, 112, 100, 102, 105, 100, 8, 49, 49, 50, 51, 16, 115, 109, 97, 99, 100, 45, 105, 100, 6, 49, 55, 54, 24, 115, 97, 116, 101, 108, 108, 105, 116, 101, 45, 105, 100, 2, 52, 16, 109, 97, 99, 45, 97, 100, 100, 114, 24, 48, 48, 97, 48, 98, 99, 56, 54, 57, 56, 100, 52, 10, 115, 116, 97, 116, 101, 14, 111, 110, 95, 108, 105, 110, 101, 12, 118, 110, 111, 45, 105, 100, 16, 101, 120, 101, 100, 101, 114, 101, 115, 14, 98, 101, 97, 109, 45, 105, 100, 10, 49, 48, 51, 56, 54, 22, 99, 97, 114, 114, 105, 101, 114, 100, 45, 105, 100, 2, 55, 44, 115, 101, 114, 118, 105, 110, 103, 45, 115, 109, 97, 99, 45, 104, 111, 115, 116, 45, 110, 97, 109, 101, 38, 115, 109, 97, 99, 45, 99, 104, 105, 49, 50, 45, 110, 49, 45, 97, 108, 112, 104, 97, 0, 0];
//    let record : [u8; 257] = [0, 0, 0, 2, 106, 0, 186, 149, 235, 179, 11, 86, 118, 105, 97, 115, 97, 116, 45, 97, 98, 45, 118, 110, 111, 45, 112, 109, 46, 117, 116, 46, 112, 100, 102, 46, 102, 108, 45, 115, 100, 117, 45, 109, 97, 114, 107, 101, 100, 45, 99, 111, 117, 110, 116, 0, 0, 2, 22, 10, 97, 110, 45, 105, 100, 2, 49, 10, 112, 100, 102, 105, 100, 8, 49, 48, 53, 50, 16, 115, 109, 97, 99, 100, 45, 105, 100, 6, 49, 52, 55, 24, 115, 97, 116, 101, 108, 108, 105, 116, 101, 45, 105, 100, 2, 52, 34, 115, 109, 97, 99, 45, 115, 101, 114, 118, 105, 99, 101, 45, 110, 97, 109, 101, 26, 115, 109, 97, 99, 45, 99, 104, 105, 48, 55, 45, 115, 50, 16, 109, 97, 99, 45, 97, 100, 100, 114, 24, 48, 48, 97, 48, 98, 99, 56, 99, 55, 57, 55, 102, 10, 115, 116, 97, 116, 101, 14, 111, 110, 95, 108, 105, 110, 101, 14, 98, 101, 97, 109, 45, 105, 100, 10, 49, 49, 48, 52, 53, 22, 99, 97, 114, 114, 105, 101, 114, 100, 45, 105, 100, 2, 55, 12, 118, 110, 111, 45, 105, 100, 6, 120, 99, 105, 44, 115, 101, 114, 118, 105, 110, 103, 45, 115, 109, 97, 99, 45, 104, 111, 115, 116, 45, 110, 97, 109, 101, 36, 115, 109, 97, 99, 45, 99, 104, 105, 48, 55, 45, 110, 50, 45, 98, 101, 116, 97, 0, 0];
//
//    let mut offset : usize = 3;
//
//    let record = &record[offset..];
//    let (tsvariant,record) = visit_varint(record);
//    println!("tsvariant: {}", tsvariant);
//
//    let (ts,record) = visit_u64(record);
//    println!("ts: {}", ts);
//
//    let (metric, record) = visit_str(record);
//    println!("metric: {}", String::from_utf8_lossy(metric));
//
//    let (vvariant, record) = visit_varint(record);
//    println!("value variant: {}", vvariant);
//
//    let (value, record) = visit_long(record);
//    println!("value: {}", value);
//
//    let (tagvariant, record) = visit_varint(record);
//    println!("tag variant: {}", tagvariant);
//
//    let (strmap, record) = visit_strmap(record);
//    for (key,val) in strmap.iter() {
//        println!("  key: {}, val: {}",
//            String::from_utf8_lossy(key),
//            String::from_utf8_lossy(val))
//    }
//
//    let (mdvariant,record) = visit_varint(record);
//    println!("metadata variant: {}", mdvariant);
//
//    println!("leftovers: {:#?}", record);
//
//    assert_eq!(true, false);
//}

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

struct BabyMapVisitor<'a, 'de: 'a> {
    de: &'a mut AvroDeserializer<'de>,
    index: usize
}

impl<'de, 'a> serde::de::MapAccess<'de> for BabyMapVisitor<'a, 'de> {
    type Error = AvroError;

    fn next_key_seed<K>(&mut self, seed: K) -> Result<Option<K::Value>, Self::Error>
        where
            K: serde::de::DeserializeSeed<'de> {
        println!("next_key_seed");
        seed.deserialize(&mut *self.de).map(Some)

    }

    fn next_value_seed<V>(&mut self, seed: V) -> Result<V::Value, Self::Error>
        where
            V: serde::de::DeserializeSeed<'de> {
        println!("next_value_seed");
        seed.deserialize(&mut *self.de)
    }
}

struct BabyEnumVisitor<'a, 'de: 'a> {
    de: &'a mut AvroDeserializer<'de>,
    enum_name: &'static str,
    enum_variants: &'a [&'static str],
    is_inside_enum: bool,
}

impl<'a, 'de> BabyEnumVisitor<'a, 'de> {
    fn new(de: &'a mut AvroDeserializer<'de>, enum_name: &'static str, enum_variants: &'a[&'static str]) -> Self {
        Self { de, enum_name, enum_variants, is_inside_enum: false }
    }
}

// `EnumAccess` is provided to the `Visitor` to give it the ability to determine
// which variant of the enum is supposed to be deserialized.
//
// Note that all enum deserialization methods in Serde refer exclusively to the
// "externally tagged" enum representation.
impl<'de, 'a> EnumAccess<'de> for BabyEnumVisitor<'a, 'de> {
    type Error = AvroError;
    type Variant = Self;

    fn variant_seed<V>(self, seed: V) -> Result<(V::Value, Self::Variant), Self::Error>
        where
            V: serde::de::DeserializeSeed<'de>,
    {
        println!("EnumAccess::variant_seed");
//        let variant = self.de.visit_varint();
//        println!("variant: {}", variant);
//        let the_type = &self.de.current_field().types[variant as usize];
//        panic!("hello")

        // The `deserialize_enum` method parsed a `{` character so we are
        // currently inside of a map. The seed will be deserializing itself from
        // the key of the map.
        let variant = self.de.visit_varint();

        println!("variant: {}", variant);

        let val = match seed.deserialize(&mut *self.de) {
            Ok(t) => t,
            Err(e) => {
                println!("error! {:#?}", e);
                panic!("hi");
            }
        };

        Ok((val,self))
//        // Parse the colon separating map key from value.
//        if self.de.next_char()? == ':' {
//            Ok((val, self))
//        } else {
//            Err(Error::ExpectedMapColon)
//        }
    }
}

// `VariantAccess` is provided to the `Visitor` to give it the ability to see
// the content of the single variant that it decided to deserialize.
impl<'de, 'a> serde::de::VariantAccess<'de> for BabyEnumVisitor<'a, 'de> {
    type Error = AvroError;

    // If the `Visitor` expected this variant to be a unit variant, the input
    // should have been the plain string case handled in `deserialize_enum`.
    fn unit_variant(self) -> Result<(),Self::Error> {
//        Err(Error::ExpectedString)
        panic!("unit variant")
    }

    // Newtype variants are represented in JSON as `{ NAME: VALUE }` so
    // deserialize the value here.
    fn newtype_variant_seed<T>(self, seed: T) -> Result<T::Value, AvroError>
        where
            T: serde::de::DeserializeSeed<'de>,
    {
        panic!("hey")
//        seed.deserialize(self.de)
    }

    // Tuple variants are represented in JSON as `{ NAME: [DATA...] }` so
    // deserialize the sequence of data here.
    fn tuple_variant<V>(self, _len: usize, visitor: V) -> Result<V::Value, AvroError>
        where
            V: Visitor<'de>,
    {
        panic!("supppp")
//        de::Deserializer::deserialize_seq(self.de, visitor)
    }

    // Struct variants are represented in JSON as `{ NAME: { K: V, ... } }` so
    // deserialize the inner map here.
    fn struct_variant<V>(
        self,
        _fields: &'static [&'static str],
        visitor: V,
    ) -> Result<V::Value, AvroError>
        where
            V: Visitor<'de>,
    {
        panic!("neato")
//        de::Deserializer::deserialize_map(self.de, visitor)
    }
}


impl<'de, 'a> Deserializer<'de> for &'a mut AvroDeserializer<'de> {
    type Error = AvroError;

    fn deserialize_any<V>(mut self, visitor: V) -> Result<V::Value,Self::Error>
        where V: Visitor<'de> {
        println!("deserialize_any");

        self.skip(3);

        let mut fields = self.visitor.fields.clone();
        let mut fields_iter = fields.iter();
        if let Some(f) = fields_iter.next() {
            let vis = match &f.types {
                AvroTypeOneOrMany::Many(ref list) => {
                    let varint = AvroDeserializer::visit_varint(&mut self);
                    println!("varint: {}", varint);
                    let variant = &list[varint as usize];
                    match variant {
                        AvroType::Primitive(AvroPrimitiveFields::Int)=> {
                            let value = AvroDeserializer::visit_varint(&mut self);
                            visitor.visit_i64::<AvroError>(value)
                        },
                        other => {
                            panic!("huh")
                        }
                    }
                },
                other => {
                    panic!("other!!")
                }
            };

            self.current_field_index += 1;
            vis
        } else {
            panic!("else")
        }
    }

    fn deserialize_struct<V>(mut self, _id: &'static str, _fields: &'static[&'static str], visitor: V) -> Result<V::Value,Self::Error>
        where V: Visitor<'de> {
        println!("deserialize_struct");

        self.deserialize_map(visitor)
    }

    fn deserialize_map<V>(mut self, visitor: V) -> Result<V::Value,Self::Error>
        where V: Visitor<'de> {
        println!("deserialize_map");

        let baby_map : BabyMapVisitor = BabyMapVisitor{de: &mut self, index: 0};

//        unimplemented!()

        visitor.visit_map(baby_map)
    }

    fn deserialize_identifier<V>(mut self, visitor: V) -> Result<V::Value, Self::Error>
        where V: Visitor<'de> {
//        let field = self.current_field();
//        let variant = match &field.types {
//            AvroTypeOneOrMany::Many(_) => {
//                self.visit_varint()
//            },
//            other => {
//                panic!("someday")
//            }
//        };

        let current_field = self.current_field();
        println!("deserialize_identifier {}", current_field.name);

        visitor.visit_string(current_field.name.clone())
    }

    fn deserialize_enum<V>(mut self, enum_name: &'static str, enum_variants: &[&'static str], visitor: V) -> Result<V::Value, Self::Error>
        where V: Visitor<'de> {
//        visitor.visit_borrowed_str(boos[variant as usize])
        let field = self.current_field();
        println!("deserialize_enum");

        match field.types {
            AvroTypeOneOrMany::One(_) => {
                panic!("what")
            },
            AvroTypeOneOrMany::Many(ref types) => {
                println!("visiting enum");
                let variant = self.visit_varint();
                let value = visitor.visit_enum(BabyEnumVisitor::new(self, enum_name, enum_variants) )?;
                Ok(value)
            }
        }
    }

    forward_to_deserialize_any!{
        <V: Visitor<'de>>
        bool i8 i16 i32 i64 u8 u16 u32 u64 f32 f64 char str string bytes byte_buf option unit unit_struct newtype_struct seq tuple tuple_struct ignored_any
    }
}

impl<'de> AvroDeserializer<'de> {
    fn dump(&self) {
        println!("{:#?}", self.buf);
    }

    fn skip(&mut self, bytes: usize) {
        self.buf = &self.buf[bytes..];
    }

    fn visit_u64(&mut self) -> u64 {
        use byteorder::{ ByteOrder, LittleEndian };
        let val = LittleEndian::read_u64(&self.buf[..8]);
        self.buf = &self.buf[7..];
        val
    }

    fn visit_varint(&mut self) -> i64 {
        let (int,varsize) : (u64, usize) = decode_var(self.buf);
        self.buf = &self.buf[varsize..];
        int as i64
    }

    fn visit_long(&mut self) -> i64 {
        self.visit_varint()
    }

    fn visit_str(&mut self) -> &'de [u8] {
        let (strlen,strstart) : (u64, usize) = decode_var(&self.buf[..]);

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
        let current_field_index = 0;
        AvroDeserializer {
            buf,
            visitor,
            current_field_index,
        }
    }

    fn current_field(&self) -> &AvroField {
        &self.visitor.fields[self.current_field_index]
    }

    fn done_with_filed(&mut self) {
        self.current_field_index += 1;
    }
}

#[derive(Deserialize,Debug)]
struct UT {
    timestamp: Timestamp,
//    metric: String,
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

fn visit_u64(buf: &[u8]) -> (u64, &[u8]) {
    use byteorder::{ ByteOrder, LittleEndian };
    let val = LittleEndian::read_u64(&buf[..8]);
    (val,&buf[7..]) // TODO: wtf is going on here?
}

fn visit_varint(buf: &[u8]) -> (i64, &[u8]) {
    let (int,varsize) : (u64, usize) = decode_var(buf);
    (int as i64, &buf[varsize..])
}

fn visit_long(buf: &[u8]) -> (i64, &[u8]) {
    visit_varint(buf)
}

fn visit_str(buf: &[u8]) -> (&[u8], &[u8]) {
    let (strlen,strstart) : (u64, usize) = decode_var(&buf[..]);

    let rstr = &buf[strstart..strstart+strlen as usize];
    let rest = &buf[strstart+strlen as usize..];

    return (rstr, rest)
}

fn visit_strmap(buf: &[u8]) -> (Vec<(&[u8],&[u8])>, &[u8]) {
    let (num_blocks,mut buf) = visit_varint(buf);
    println!("num_blocks: {}", num_blocks);

    let mut vec : Vec<(&[u8], &[u8])> = Vec::with_capacity(num_blocks as usize);

    for _i in 0..num_blocks {
        let (key, val) : (&[u8], &[u8]);

        let visit = visit_str(buf);
        key = visit.0; buf = visit.1;

        let visit = visit_str(buf);
        val = visit.0; buf = visit.1;

        vec.push((key,val));
    }

    (vec,buf)
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