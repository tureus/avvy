extern crate env_logger;

extern crate serde;
#[macro_use] extern crate serde_derive;
extern crate avvy;
extern crate fnv;

use serde::de::Deserialize;

pub const SCHEMA_STR: &'static str = r###"{
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

#[derive(Deserialize,Debug)]
pub struct UT<'a> {
    pub timestamp: Timestamp,
    pub metric: &'a str,
    pub value: Value,
    #[serde(borrow)]
    pub tags: Option<Vec<(&'a str, &'a str)>>,
    #[serde(borrow)]
    pub metadata: Option<Vec<(&'a str, &'a str)>>
}

#[derive(Deserialize,Debug)]
pub struct UTSafe {
    pub timestamp: Timestamp,
    pub metric: String,
    pub value: Value,
}

#[derive(Deserialize,Debug)]
pub enum Timestamp {
    Long(i64),
    Int(i32),
    Float(f32),
    Double(f64)
}

#[derive(Deserialize,Debug)]
pub enum Value {
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

fn main() {
    env_logger::init();

    let tests = test_data();
    let test = &tests[0][..];
    let schema = avvy::Schema::from_str(SCHEMA_STR).unwrap();

    for _ in 1..1000000000 {
        let mut deserializer = avvy::AvroDeserializer{ buf: test, current_field_index: None, schema: &schema};
        deserializer.skip(5);
        UT::deserialize(&mut deserializer).unwrap();
    }
}

fn test_data() -> Vec<Vec<u8>> {
    vec![
        vec![0, 0, 0, 2, 106, 0, 184, 134, 180, 181, 11, 84, 118, 105, 97, 115, 97, 116, 45, 97, 98, 45, 118, 110, 111, 45, 112, 109, 46, 117, 116, 46, 114, 108, 45, 115, 121, 109, 98, 111, 108, 45, 116, 114, 97, 102, 102, 105, 99, 45, 114, 97, 116, 101, 6, 0, 0, 0, 0, 0, 0, 0, 0, 2, 18, 10, 97, 110, 45, 105, 100, 2, 49, 16, 115, 109, 97, 99, 100, 45, 105, 100, 6, 49, 52, 55, 24, 115, 97, 116, 101, 108, 108, 105, 116, 101, 45, 105, 100, 2, 52, 16, 109, 97, 99, 45, 97, 100, 100, 114, 24, 48, 48, 97, 48, 98, 99, 56, 99, 56, 52, 49, 49, 10, 115, 116, 97, 116, 101, 14, 114, 97, 110, 103, 105, 110, 103, 12, 118, 110, 111, 45, 105, 100, 0, 14, 98, 101, 97, 109, 45, 105, 100, 10, 49, 49, 48, 52, 53, 22, 99, 97, 114, 114, 105, 101, 114, 100, 45, 105, 100, 2, 55, 44, 115, 101, 114, 118, 105, 110, 103, 45, 115, 109, 97, 99, 45, 104, 111, 115, 116, 45, 110, 97, 109, 101, 38, 115, 109, 97, 99, 45, 99, 104, 105, 48, 55, 45, 110, 49, 45, 97, 108, 112, 104, 97, 0, 0],
        vec![0, 0, 0, 2, 106, 0, 250, 224, 155, 181, 11, 92, 118, 105, 97, 115, 97, 116, 45, 97, 98, 45, 118, 110, 111, 45, 112, 109, 46, 117, 116, 46, 112, 100, 102, 46, 102, 108, 45, 115, 121, 109, 98, 111, 108, 45, 116, 114, 97, 102, 102, 105, 99, 45, 114, 97, 116, 101, 6, 0, 0, 0, 0, 0, 128, 104, 64, 2, 20, 10, 97, 110, 45, 105, 100, 2, 49, 10, 112, 100, 102, 105, 100, 8, 49, 48, 52, 54, 16, 115, 109, 97, 99, 100, 45, 105, 100, 6, 49, 54, 57, 24, 115, 97, 116, 101, 108, 108, 105, 116, 101, 45, 105, 100, 2, 52, 16, 109, 97, 99, 45, 97, 100, 100, 114, 24, 48, 48, 97, 48, 98, 99, 55, 97, 57, 55, 55, 98, 10, 115, 116, 97, 116, 101, 14, 111, 110, 95, 108, 105, 110, 101, 12, 118, 110, 111, 45, 105, 100, 16, 101, 120, 101, 100, 101, 114, 101, 115, 14, 98, 101, 97, 109, 45, 105, 100, 10, 49, 48, 52, 48, 52, 22, 99, 97, 114, 114, 105, 101, 114, 100, 45, 105, 100, 2, 55, 44, 115, 101, 114, 118, 105, 110, 103, 45, 115, 109, 97, 99, 45, 104, 111, 115, 116, 45, 110, 97, 109, 101, 36, 115, 109, 97, 99, 45, 99, 104, 105, 48, 53, 45, 110, 49, 45, 98, 101, 116, 97, 0, 0],
    ]
}