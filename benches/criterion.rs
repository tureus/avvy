#[macro_use] extern crate criterion;
extern crate serde;
#[macro_use] extern crate serde_derive;
extern crate avvy;
extern crate fnv;

use criterion::Criterion;

use serde::de::Deserialize;

use avvy::{ Schema, AvroDeserializer };

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
    timestamp: Timestamp,
    pub metric: String,
    value: Value,
    #[serde(borrow)]
    tags: Option<Vec<(&'a [u8], &'a [u8])>>,
//    tags: Option<fnv::FnvHashMap<&'a [u8], &'a [u8]>>,
//    tags: Option<std::collections::BTreeMap<&'a [u8], &'a [u8]>>,
    #[serde(borrow)]
    metadata: Option<Vec<(&'a [u8], &'a [u8])>>,
//    metadata: Option<fnv::FnvHashMap<&'a [u8], &'a [u8]>>,
//    metadata: Option<std::collections::BTreeMap<&'a [u8], &'a [u8]>>
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

fn criterion_benchmark(c: &mut Criterion) {
    c.bench_function("parse ut", |b| {

        let record : [u8; 257] = [0, 0, 0, 2, 106, 0, 186, 149, 235, 179, 11, 86, 118, 105, 97, 115, 97, 116, 45, 97, 98, 45, 118, 110, 111, 45, 112, 109, 46, 117, 116, 46, 112, 100, 102, 46, 102, 108, 45, 115, 100, 117, 45, 109, 97, 114, 107, 101, 100, 45, 99, 111, 117, 110, 116, 0, 0, 2, 22, 10, 97, 110, 45, 105, 100, 2, 49, 10, 112, 100, 102, 105, 100, 8, 49, 48, 53, 50, 16, 115, 109, 97, 99, 100, 45, 105, 100, 6, 49, 52, 55, 24, 115, 97, 116, 101, 108, 108, 105, 116, 101, 45, 105, 100, 2, 52, 34, 115, 109, 97, 99, 45, 115, 101, 114, 118, 105, 99, 101, 45, 110, 97, 109, 101, 26, 115, 109, 97, 99, 45, 99, 104, 105, 48, 55, 45, 115, 50, 16, 109, 97, 99, 45, 97, 100, 100, 114, 24, 48, 48, 97, 48, 98, 99, 56, 99, 55, 57, 55, 102, 10, 115, 116, 97, 116, 101, 14, 111, 110, 95, 108, 105, 110, 101, 14, 98, 101, 97, 109, 45, 105, 100, 10, 49, 49, 48, 52, 53, 22, 99, 97, 114, 114, 105, 101, 114, 100, 45, 105, 100, 2, 55, 12, 118, 110, 111, 45, 105, 100, 6, 120, 99, 105, 44, 115, 101, 114, 118, 105, 110, 103, 45, 115, 109, 97, 99, 45, 104, 111, 115, 116, 45, 110, 97, 109, 101, 36, 115, 109, 97, 99, 45, 99, 104, 105, 48, 55, 45, 110, 50, 45, 98, 101, 116, 97, 0, 0];
        let visitor = Schema::from_str(SCHEMA_STR).unwrap();

        b.iter(|| {
            let mut deserializer = AvroDeserializer::from_slice( &visitor,&record[..]);
            deserializer.skip(5);

            let _ : UT = UT::deserialize(&mut deserializer).unwrap();
        })
    });
}

criterion_group!(benches, criterion_benchmark);
criterion_main!(benches);