#[macro_use] extern crate criterion;
extern crate serde;
#[macro_use] extern crate serde_derive;
extern crate avvy;
extern crate fnv;
extern crate smallvec;

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


macro_rules! ut_struct {
        ($i:ident, $e:ty) => {
            #[derive(Deserialize,Debug,Clone)]
            pub struct $i<'a> {
                timestamp: Timestamp,
                pub metric: String,
                value: Value,
                #[serde(borrow)]
                tags: $e,
                #[serde(borrow)]
                metadata: $e,
            }
        }
}

macro_rules! ut_noborrow_struct {
        ($i:ident, $e:ty) => {
            #[derive(Deserialize,Debug,Clone)]
            pub struct $i {
                timestamp: Timestamp,
                pub metric: String,
                value: Value,
                tags: $e,
                metadata: $e,
            }
        }
}

ut_struct!(UTSmallVec, Option<smallvec::SmallVec<[(&'a [u8], &'a [u8]); 15]>>);
ut_struct!(UTVec, Option< Vec<(&'a [u8], &'a [u8])> >);
ut_noborrow_struct!(UTVecString, Option< Vec<(String, String)> >);
ut_struct!(UTFNV, Option<fnv::FnvHashMap<&'a [u8], &'a [u8]>>);
ut_struct!(UTBTreeMap, Option<std::collections::BTreeMap<&'a [u8], &'a [u8]>>);
ut_struct!(UTHashMap, Option<std::collections::HashMap<&'a [u8], &'a [u8]>>);

#[derive(Deserialize,Debug,Clone)]
pub struct UTUnsafeUTF8<'a> {
    timestamp: Timestamp,
    pub metric: String,
    value: Value,
    #[serde(borrow)]
    tags: Option<Vec<(&'a str, &'a str)>>,
    #[serde(borrow)]
    metadata: Option<Vec<(&'a str, &'a str)>>,
}

#[derive(Deserialize,Debug,Clone)]
enum Timestamp {
    Long(i64),
    Int(i32),
    Float(f32),
    Double(f64)
}

#[derive(Deserialize,Debug,Clone)]
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

macro_rules! ut_test {
    ($name:expr, $i:ident, $bench:expr) => {
            $bench.bench_function($name, |b| {

                let record : [u8; 257] = [0, 0, 0, 2, 106, 0, 186, 149, 235, 179, 11, 86, 118, 105, 97, 115, 97, 116, 45, 97, 98, 45, 118, 110, 111, 45, 112, 109, 46, 117, 116, 46, 112, 100, 102, 46, 102, 108, 45, 115, 100, 117, 45, 109, 97, 114, 107, 101, 100, 45, 99, 111, 117, 110, 116, 0, 0, 2, 22, 10, 97, 110, 45, 105, 100, 2, 49, 10, 112, 100, 102, 105, 100, 8, 49, 48, 53, 50, 16, 115, 109, 97, 99, 100, 45, 105, 100, 6, 49, 52, 55, 24, 115, 97, 116, 101, 108, 108, 105, 116, 101, 45, 105, 100, 2, 52, 34, 115, 109, 97, 99, 45, 115, 101, 114, 118, 105, 99, 101, 45, 110, 97, 109, 101, 26, 115, 109, 97, 99, 45, 99, 104, 105, 48, 55, 45, 115, 50, 16, 109, 97, 99, 45, 97, 100, 100, 114, 24, 48, 48, 97, 48, 98, 99, 56, 99, 55, 57, 55, 102, 10, 115, 116, 97, 116, 101, 14, 111, 110, 95, 108, 105, 110, 101, 14, 98, 101, 97, 109, 45, 105, 100, 10, 49, 49, 48, 52, 53, 22, 99, 97, 114, 114, 105, 101, 114, 100, 45, 105, 100, 2, 55, 12, 118, 110, 111, 45, 105, 100, 6, 120, 99, 105, 44, 115, 101, 114, 118, 105, 110, 103, 45, 115, 109, 97, 99, 45, 104, 111, 115, 116, 45, 110, 97, 109, 101, 36, 115, 109, 97, 99, 45, 99, 104, 105, 48, 55, 45, 110, 50, 45, 98, 101, 116, 97, 0, 0];
                let visitor = Schema::from_str(SCHEMA_STR).unwrap();

                b.iter(|| {
                    let mut deserializer = AvroDeserializer::from_slice( &visitor,&record[..]);
                    deserializer.skip(5);

                    let _ = $i::deserialize(&mut deserializer).unwrap();
                })
            });
    }
}

fn ut_deserializer_benchmark(c: &mut Criterion) {
    ut_test!("UTSmallVec", UTSmallVec, c);
    ut_test!("UTVec", UTVec, c);
    ut_test!("UTVecString", UTVecString, c);
    ut_test!("UTFNV", UTFNV, c);
    ut_test!("UTBTreeMap", UTBTreeMap, c);
    ut_test!("UTHashMap", UTHashMap, c);
    ut_test!("UTUnsafeUTF8", UTUnsafeUTF8, c);
}

fn ut_vec_string_conversion_benchmark(c: &mut Criterion) {
    c.bench_function("serialize for String-deserialized influxdb", |b| {
        let record : [u8; 257] = [0, 0, 0, 2, 106, 0, 186, 149, 235, 179, 11, 86, 118, 105, 97, 115, 97, 116, 45, 97, 98, 45, 118, 110, 111, 45, 112, 109, 46, 117, 116, 46, 112, 100, 102, 46, 102, 108, 45, 115, 100, 117, 45, 109, 97, 114, 107, 101, 100, 45, 99, 111, 117, 110, 116, 0, 0, 2, 22, 10, 97, 110, 45, 105, 100, 2, 49, 10, 112, 100, 102, 105, 100, 8, 49, 48, 53, 50, 16, 115, 109, 97, 99, 100, 45, 105, 100, 6, 49, 52, 55, 24, 115, 97, 116, 101, 108, 108, 105, 116, 101, 45, 105, 100, 2, 52, 34, 115, 109, 97, 99, 45, 115, 101, 114, 118, 105, 99, 101, 45, 110, 97, 109, 101, 26, 115, 109, 97, 99, 45, 99, 104, 105, 48, 55, 45, 115, 50, 16, 109, 97, 99, 45, 97, 100, 100, 114, 24, 48, 48, 97, 48, 98, 99, 56, 99, 55, 57, 55, 102, 10, 115, 116, 97, 116, 101, 14, 111, 110, 95, 108, 105, 110, 101, 14, 98, 101, 97, 109, 45, 105, 100, 10, 49, 49, 48, 52, 53, 22, 99, 97, 114, 114, 105, 101, 114, 100, 45, 105, 100, 2, 55, 12, 118, 110, 111, 45, 105, 100, 6, 120, 99, 105, 44, 115, 101, 114, 118, 105, 110, 103, 45, 115, 109, 97, 99, 45, 104, 111, 115, 116, 45, 110, 97, 109, 101, 36, 115, 109, 97, 99, 45, 99, 104, 105, 48, 55, 45, 110, 50, 45, 98, 101, 116, 97, 0, 0];
        let visitor = Schema::from_str(SCHEMA_STR).unwrap();
        let mut deserializer = AvroDeserializer{buf: &record[..], schema: &visitor, current_field_index: None };
        deserializer.skip(5);
        let utvec = UTVecString::deserialize(&mut deserializer).unwrap();
        let record_count = 10000;
        let data : Vec<UTVecString> = (1..record_count).map(|_| (utvec).clone() ).collect();

        use std::io::Write;
        let mut buf = Vec::with_capacity(record_count * 262);

        b.iter(|| {
            for utvec in &data {
                write!(&mut buf, "{},", utvec.metric).unwrap();
                if let Some(ref tags) = utvec.tags {
                    for (k, v) in tags {
                        if v.len() != 0 {
                            write!(&mut buf, "{}={}", k ,v).unwrap();
                        }
                    }
                }

                write!(&mut buf, " metric={:?} ", utvec.value).unwrap();
                write!(&mut buf, "{:?}\n", utvec.timestamp).unwrap();
            }
            buf.clear();
        })
    });
}

fn ut_vec_raw_string_conversion_benchmark(c: &mut Criterion) {
    c.bench_function("serialize String::from_utf8_lossy influxdb", |b| {
        let record : [u8; 257] = [0, 0, 0, 2, 106, 0, 186, 149, 235, 179, 11, 86, 118, 105, 97, 115, 97, 116, 45, 97, 98, 45, 118, 110, 111, 45, 112, 109, 46, 117, 116, 46, 112, 100, 102, 46, 102, 108, 45, 115, 100, 117, 45, 109, 97, 114, 107, 101, 100, 45, 99, 111, 117, 110, 116, 0, 0, 2, 22, 10, 97, 110, 45, 105, 100, 2, 49, 10, 112, 100, 102, 105, 100, 8, 49, 48, 53, 50, 16, 115, 109, 97, 99, 100, 45, 105, 100, 6, 49, 52, 55, 24, 115, 97, 116, 101, 108, 108, 105, 116, 101, 45, 105, 100, 2, 52, 34, 115, 109, 97, 99, 45, 115, 101, 114, 118, 105, 99, 101, 45, 110, 97, 109, 101, 26, 115, 109, 97, 99, 45, 99, 104, 105, 48, 55, 45, 115, 50, 16, 109, 97, 99, 45, 97, 100, 100, 114, 24, 48, 48, 97, 48, 98, 99, 56, 99, 55, 57, 55, 102, 10, 115, 116, 97, 116, 101, 14, 111, 110, 95, 108, 105, 110, 101, 14, 98, 101, 97, 109, 45, 105, 100, 10, 49, 49, 48, 52, 53, 22, 99, 97, 114, 114, 105, 101, 114, 100, 45, 105, 100, 2, 55, 12, 118, 110, 111, 45, 105, 100, 6, 120, 99, 105, 44, 115, 101, 114, 118, 105, 110, 103, 45, 115, 109, 97, 99, 45, 104, 111, 115, 116, 45, 110, 97, 109, 101, 36, 115, 109, 97, 99, 45, 99, 104, 105, 48, 55, 45, 110, 50, 45, 98, 101, 116, 97, 0, 0];
        let visitor = Schema::from_str(SCHEMA_STR).unwrap();
        let mut deserializer = AvroDeserializer{buf: &record[..], schema: &visitor, current_field_index: None };
        deserializer.skip(5);
        let utvec = UTVec::deserialize(&mut deserializer).unwrap();
        let record_count = 10000;
        let data : Vec<UTVec> = (1..record_count).map(|_| (utvec).clone() ).collect();

        use std::io::Write;
        let mut buf = Vec::with_capacity(record_count * 262);

        b.iter(|| {
            for utvec in &data {
                write!(&mut buf, "{},", utvec.metric).unwrap();
                if let Some(ref tags) = utvec.tags {
                    for (k, v) in tags {
                        if v.len() != 0 {
                            write!(&mut buf, "{}={}", String::from_utf8_lossy(k) , String::from_utf8_lossy(v)).unwrap();
                        }
                    }
                }

                write!(&mut buf, " metric={:?} ", utvec.value).unwrap();
                write!(&mut buf, "{:?}\n", utvec.timestamp).unwrap();
            }
            buf.clear();
        })
    });
}


fn ut_vec_raw_buf_conversion_benchmark(c: &mut Criterion) {
    c.bench_function("serialize for std::str::from_utf8_unchecked influxdb", |b| {
        let record : [u8; 257] = [0, 0, 0, 2, 106, 0, 186, 149, 235, 179, 11, 86, 118, 105, 97, 115, 97, 116, 45, 97, 98, 45, 118, 110, 111, 45, 112, 109, 46, 117, 116, 46, 112, 100, 102, 46, 102, 108, 45, 115, 100, 117, 45, 109, 97, 114, 107, 101, 100, 45, 99, 111, 117, 110, 116, 0, 0, 2, 22, 10, 97, 110, 45, 105, 100, 2, 49, 10, 112, 100, 102, 105, 100, 8, 49, 48, 53, 50, 16, 115, 109, 97, 99, 100, 45, 105, 100, 6, 49, 52, 55, 24, 115, 97, 116, 101, 108, 108, 105, 116, 101, 45, 105, 100, 2, 52, 34, 115, 109, 97, 99, 45, 115, 101, 114, 118, 105, 99, 101, 45, 110, 97, 109, 101, 26, 115, 109, 97, 99, 45, 99, 104, 105, 48, 55, 45, 115, 50, 16, 109, 97, 99, 45, 97, 100, 100, 114, 24, 48, 48, 97, 48, 98, 99, 56, 99, 55, 57, 55, 102, 10, 115, 116, 97, 116, 101, 14, 111, 110, 95, 108, 105, 110, 101, 14, 98, 101, 97, 109, 45, 105, 100, 10, 49, 49, 48, 52, 53, 22, 99, 97, 114, 114, 105, 101, 114, 100, 45, 105, 100, 2, 55, 12, 118, 110, 111, 45, 105, 100, 6, 120, 99, 105, 44, 115, 101, 114, 118, 105, 110, 103, 45, 115, 109, 97, 99, 45, 104, 111, 115, 116, 45, 110, 97, 109, 101, 36, 115, 109, 97, 99, 45, 99, 104, 105, 48, 55, 45, 110, 50, 45, 98, 101, 116, 97, 0, 0];
        let visitor = Schema::from_str(SCHEMA_STR).unwrap();
        let mut deserializer = AvroDeserializer{buf: &record[..], schema: &visitor, current_field_index: None };
        deserializer.skip(5);
        let utvec = UTVec::deserialize(&mut deserializer).unwrap();
        let record_count = 10000;
        let data : Vec<UTVec> = (1..record_count).map(|_| (utvec).clone() ).collect();

        use std::io::Write;
        let mut buf = Vec::with_capacity(record_count * 262);

        b.iter(|| {
            for utvec in &data {
                write!(&mut buf, "{},", utvec.metric).unwrap();
                if let Some(ref tags) = utvec.tags {
                    for (k, v) in tags {
                        if v.len() != 0 {
                            unsafe {
                                write!(&mut buf, "{}={}", std::str::from_utf8_unchecked(k) , std::str::from_utf8_unchecked(v)).unwrap();
                            }
                        }
                    }
                }

                write!(&mut buf, " metric={:?} ", utvec.value).unwrap();
                write!(&mut buf, "{:?}\n", utvec.timestamp).unwrap();
            }
            buf.clear();
        })
    });
}

fn ut_vec_raw_buf_noconversion_benchmark(c: &mut Criterion) {
    c.bench_function("serialize for std::str::from_utf8_unchecked influxdb", |b| {
        let record : [u8; 257] = [0, 0, 0, 2, 106, 0, 186, 149, 235, 179, 11, 86, 118, 105, 97, 115, 97, 116, 45, 97, 98, 45, 118, 110, 111, 45, 112, 109, 46, 117, 116, 46, 112, 100, 102, 46, 102, 108, 45, 115, 100, 117, 45, 109, 97, 114, 107, 101, 100, 45, 99, 111, 117, 110, 116, 0, 0, 2, 22, 10, 97, 110, 45, 105, 100, 2, 49, 10, 112, 100, 102, 105, 100, 8, 49, 48, 53, 50, 16, 115, 109, 97, 99, 100, 45, 105, 100, 6, 49, 52, 55, 24, 115, 97, 116, 101, 108, 108, 105, 116, 101, 45, 105, 100, 2, 52, 34, 115, 109, 97, 99, 45, 115, 101, 114, 118, 105, 99, 101, 45, 110, 97, 109, 101, 26, 115, 109, 97, 99, 45, 99, 104, 105, 48, 55, 45, 115, 50, 16, 109, 97, 99, 45, 97, 100, 100, 114, 24, 48, 48, 97, 48, 98, 99, 56, 99, 55, 57, 55, 102, 10, 115, 116, 97, 116, 101, 14, 111, 110, 95, 108, 105, 110, 101, 14, 98, 101, 97, 109, 45, 105, 100, 10, 49, 49, 48, 52, 53, 22, 99, 97, 114, 114, 105, 101, 114, 100, 45, 105, 100, 2, 55, 12, 118, 110, 111, 45, 105, 100, 6, 120, 99, 105, 44, 115, 101, 114, 118, 105, 110, 103, 45, 115, 109, 97, 99, 45, 104, 111, 115, 116, 45, 110, 97, 109, 101, 36, 115, 109, 97, 99, 45, 99, 104, 105, 48, 55, 45, 110, 50, 45, 98, 101, 116, 97, 0, 0];
        let visitor = Schema::from_str(SCHEMA_STR).unwrap();
        let mut deserializer = AvroDeserializer{buf: &record[..], schema: &visitor, current_field_index: None };
        deserializer.skip(5);
        let utvec = UTUnsafeUTF8::deserialize(&mut deserializer).unwrap();
        let record_count = 10000;
        let data : Vec<UTUnsafeUTF8> = (1..record_count).map(|_| (utvec).clone() ).collect();

        use std::io::Write;
        let mut buf = Vec::with_capacity(record_count * 262);

        b.iter(|| {
            for utvec in &data {
                write!(&mut buf, "{},", utvec.metric).unwrap();
                if let Some(ref tags) = utvec.tags {
                    for (k, v) in tags {
                        if v.len() != 0 {
                            unsafe {
                                write!(&mut buf, "{}={}", k , v).unwrap();
                            }
                        }
                    }
                }

                write!(&mut buf, " metric={:?} ", utvec.value).unwrap();
                write!(&mut buf, "{:?}\n", utvec.timestamp).unwrap();
            }
            buf.clear();
        })
    });
}

fn ut_vec_raw_buf_noconversion_iterloop_benchmark(c: &mut Criterion) {
    c.bench_function("serialize for std::str::from_utf8_unchecked (iterloop) influxdb", |b| {
        let record : [u8; 257] = [0, 0, 0, 2, 106, 0, 186, 149, 235, 179, 11, 86, 118, 105, 97, 115, 97, 116, 45, 97, 98, 45, 118, 110, 111, 45, 112, 109, 46, 117, 116, 46, 112, 100, 102, 46, 102, 108, 45, 115, 100, 117, 45, 109, 97, 114, 107, 101, 100, 45, 99, 111, 117, 110, 116, 0, 0, 2, 22, 10, 97, 110, 45, 105, 100, 2, 49, 10, 112, 100, 102, 105, 100, 8, 49, 48, 53, 50, 16, 115, 109, 97, 99, 100, 45, 105, 100, 6, 49, 52, 55, 24, 115, 97, 116, 101, 108, 108, 105, 116, 101, 45, 105, 100, 2, 52, 34, 115, 109, 97, 99, 45, 115, 101, 114, 118, 105, 99, 101, 45, 110, 97, 109, 101, 26, 115, 109, 97, 99, 45, 99, 104, 105, 48, 55, 45, 115, 50, 16, 109, 97, 99, 45, 97, 100, 100, 114, 24, 48, 48, 97, 48, 98, 99, 56, 99, 55, 57, 55, 102, 10, 115, 116, 97, 116, 101, 14, 111, 110, 95, 108, 105, 110, 101, 14, 98, 101, 97, 109, 45, 105, 100, 10, 49, 49, 48, 52, 53, 22, 99, 97, 114, 114, 105, 101, 114, 100, 45, 105, 100, 2, 55, 12, 118, 110, 111, 45, 105, 100, 6, 120, 99, 105, 44, 115, 101, 114, 118, 105, 110, 103, 45, 115, 109, 97, 99, 45, 104, 111, 115, 116, 45, 110, 97, 109, 101, 36, 115, 109, 97, 99, 45, 99, 104, 105, 48, 55, 45, 110, 50, 45, 98, 101, 116, 97, 0, 0];
        let visitor = Schema::from_str(SCHEMA_STR).unwrap();
        let mut deserializer = AvroDeserializer{buf: &record[..], schema: &visitor, current_field_index: None };
        deserializer.skip(5);
        let utvec = UTUnsafeUTF8::deserialize(&mut deserializer).unwrap();
        let record_count = 10000;
        let data : Vec<UTUnsafeUTF8> = (1..record_count).map(|_| (utvec).clone() ).collect();

        use std::io::Write;
        let mut buf = Vec::with_capacity(record_count * 262);

        b.iter(|| {
            for utvec in data.iter() {
                write!(&mut buf, "{},", utvec.metric).unwrap();
                if let Some(ref tags) = utvec.tags {
                    for (k, v) in tags {
                        if v.len() != 0 {
                            unsafe {
                                write!(&mut buf, "{}={}", k , v).unwrap();
                            }
                        }
                    }
                }

                write!(&mut buf, " metric={:?} ", utvec.value).unwrap();
                write!(&mut buf, "{:?}\n", utvec.timestamp).unwrap();
            }
            buf.clear();
        })
    });
}

fn ut_vec_raw_buf_forloop2_conversion_benchmark(c: &mut Criterion) {
    c.bench_function("serialize for std::str::from_utf8_unchecked (borrowed for loop) influxdb", |b| {
        let record : [u8; 257] = [0, 0, 0, 2, 106, 0, 186, 149, 235, 179, 11, 86, 118, 105, 97, 115, 97, 116, 45, 97, 98, 45, 118, 110, 111, 45, 112, 109, 46, 117, 116, 46, 112, 100, 102, 46, 102, 108, 45, 115, 100, 117, 45, 109, 97, 114, 107, 101, 100, 45, 99, 111, 117, 110, 116, 0, 0, 2, 22, 10, 97, 110, 45, 105, 100, 2, 49, 10, 112, 100, 102, 105, 100, 8, 49, 48, 53, 50, 16, 115, 109, 97, 99, 100, 45, 105, 100, 6, 49, 52, 55, 24, 115, 97, 116, 101, 108, 108, 105, 116, 101, 45, 105, 100, 2, 52, 34, 115, 109, 97, 99, 45, 115, 101, 114, 118, 105, 99, 101, 45, 110, 97, 109, 101, 26, 115, 109, 97, 99, 45, 99, 104, 105, 48, 55, 45, 115, 50, 16, 109, 97, 99, 45, 97, 100, 100, 114, 24, 48, 48, 97, 48, 98, 99, 56, 99, 55, 57, 55, 102, 10, 115, 116, 97, 116, 101, 14, 111, 110, 95, 108, 105, 110, 101, 14, 98, 101, 97, 109, 45, 105, 100, 10, 49, 49, 48, 52, 53, 22, 99, 97, 114, 114, 105, 101, 114, 100, 45, 105, 100, 2, 55, 12, 118, 110, 111, 45, 105, 100, 6, 120, 99, 105, 44, 115, 101, 114, 118, 105, 110, 103, 45, 115, 109, 97, 99, 45, 104, 111, 115, 116, 45, 110, 97, 109, 101, 36, 115, 109, 97, 99, 45, 99, 104, 105, 48, 55, 45, 110, 50, 45, 98, 101, 116, 97, 0, 0];
        let visitor = Schema::from_str(SCHEMA_STR).unwrap();
        let mut deserializer = AvroDeserializer{buf: &record[..], schema: &visitor, current_field_index: None };
        deserializer.skip(5);
        let utvec = UTVec::deserialize(&mut deserializer).unwrap();
        let record_count = 10000;
        let data : Vec<UTVec> = (1..record_count).map(|_| (utvec).clone() ).collect();

        use std::io::Write;
        let mut buf = Vec::with_capacity(record_count * 262);

        b.iter(|| {
            for utvec in &data {
                write!(&mut buf, "{},", utvec.metric).unwrap();
                if let Some(ref tags) = utvec.tags {
                    for (k, v) in tags.iter() {
                        if v.len() != 0 {
                            unsafe {
                                write!(&mut buf, "{}={}", std::str::from_utf8_unchecked(k) , std::str::from_utf8_unchecked(v)).unwrap();
                            }
                        }
                    }
                }

                write!(&mut buf, " metric={:?} ", utvec.value).unwrap();
                write!(&mut buf, "{:?}\n", utvec.timestamp).unwrap();
            }
            buf.clear();
        })
    });
}

criterion_group!(benches, ut_deserializer_benchmark, ut_vec_string_conversion_benchmark, ut_vec_raw_string_conversion_benchmark, ut_vec_raw_buf_conversion_benchmark, ut_vec_raw_buf_forloop2_conversion_benchmark, ut_vec_raw_buf_noconversion_benchmark, ut_vec_raw_buf_noconversion_iterloop_benchmark);
criterion_main!(benches);
