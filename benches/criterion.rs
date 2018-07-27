#[macro_use] extern crate criterion;
extern crate serde;
#[macro_use] extern crate serde_derive;
extern crate avvy;
extern crate fnv;
extern crate smallvec;

use criterion::Criterion;

use serde::de::Deserialize;

use avvy::{ Schema, AvroDeserializer };

pub trait InfluxDBLineProtocol<W: std::io::Write> {
    fn to_line_protocol(&self, writer: &mut W) -> Result<(),std::io::Error>;
}

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
                pub metric: &'a str,
                value: Value,
                #[serde(borrow)]
                tags: $e,
                #[serde(borrow)]
                metadata: $e,
            }

            impl<'a, W: std::io::Write> InfluxDBLineProtocol<W> for $i<'a> {
                fn to_line_protocol(&self, buf: &mut W) -> Result<(),std::io::Error> {
                    write!(buf, "{},", self.metric).unwrap();
                    if let Some(ref tags) = self.tags {
                        for (k, v) in tags {
                            if v.len() != 0 {
                                write!(buf, "{}={}", k , v)?;
                            }
                        }
                    }

                    match self.value {
                        Value::Long(ref val) => write!(buf, " metric={}", val)?,
                        Value::Int(ref val) => write!(buf, " metric={}", val)?,
                        Value::Float(ref val) => write!(buf, " metric={}", val)?,
                        Value::Double(ref val) => write!(buf, " metric={}", val)?,
                        Value::Long8(ref val) => write!(buf, " metric={}", val)?,
                        Value::Long16(ref val) => write!(buf, " metric={}", val)?,
                        Value::Long32(ref val) => write!(buf, " metric={}", val)?,
                        Value::Long64(ref val) => write!(buf, " metric={}", val)?,
                        Value::I8(ref val) => write!(buf, " metric={}", val)?,
                        Value::I16(ref val) => write!(buf, " metric={}", val)?,
                        Value::I32(ref val) => write!(buf, " metric={}", val)?,
                        Value::Int64(ref val) => write!(buf, " metric={}", val)?
                    };

                    match self.timestamp {
                        Timestamp::Long(ref val) => write!(buf, "{}\n", val)?,
                        Timestamp::Int(ref val) => write!(buf, "{}\n", val)?,
                        Timestamp::Float(ref val) => write!(buf, "{}\n", val)?,
                        Timestamp::Double(ref val) => write!(buf, "{}\n", val)?
                    };

                    Ok(())
                }
            }
        }
}

macro_rules! ut_noborrow_struct {
        ($i:ident, $e:ty) => {
            #[derive(Deserialize,Debug,Clone)]
            pub struct $i<'a> {
                timestamp: Timestamp,
                pub metric: &'a str,
                value: Value,
                tags: $e,
                metadata: $e,
            }

            impl<'a, W: std::io::Write> InfluxDBLineProtocol<W> for $i<'a> {
                fn to_line_protocol(&self, buf: &mut W) -> Result<(),std::io::Error> {
                    write!(buf, "{},", self.metric).unwrap();
                    if let Some(ref tags) = self.tags {
                        for (k, v) in tags {
                            if v.len() != 0 {
                                write!(buf, "{}={}", k , v)?;
                            }
                        }
                    }

                    match self.value {
                        Value::Long(ref val) => write!(buf, " metric={}", val)?,
                        Value::Int(ref val) => write!(buf, " metric={}", val)?,
                        Value::Float(ref val) => write!(buf, " metric={}", val)?,
                        Value::Double(ref val) => write!(buf, " metric={}", val)?,
                        Value::Long8(ref val) => write!(buf, " metric={}", val)?,
                        Value::Long16(ref val) => write!(buf, " metric={}", val)?,
                        Value::Long32(ref val) => write!(buf, " metric={}", val)?,
                        Value::Long64(ref val) => write!(buf, " metric={}", val)?,
                        Value::I8(ref val) => write!(buf, " metric={}", val)?,
                        Value::I16(ref val) => write!(buf, " metric={}", val)?,
                        Value::I32(ref val) => write!(buf, " metric={}", val)?,
                        Value::Int64(ref val) => write!(buf, " metric={}", val)?
                    };

                    match self.timestamp {
                        Timestamp::Long(ref val) => write!(buf, "{}\n", val)?,
                        Timestamp::Int(ref val) => write!(buf, "{}\n", val)?,
                        Timestamp::Float(ref val) => write!(buf, "{}\n", val)?,
                        Timestamp::Double(ref val) => write!(buf, "{}\n", val)?
                    };

                    Ok(())
                }
            }
        }
}

ut_struct!(UTSmallVec, Option<smallvec::SmallVec<[(&'a str, &'a str); 15]>>);
ut_struct!(UTVec, Option< Vec<(&'a str, &'a str)> >);
ut_noborrow_struct!(UTVecString, Option< Vec<(String, String)> >);
ut_struct!(UTFNV, Option<fnv::FnvHashMap<&'a str, &'a str>>);
ut_struct!(UTBTreeMap, Option<std::collections::BTreeMap<&'a str, &'a str>>);
ut_struct!(UTHashMap, Option<std::collections::HashMap<&'a str, &'a str>>);

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

        let mut buf = Vec::with_capacity(record_count * 262);

        b.iter(|| {
            for utvec in &data {
                utvec.to_line_protocol(&mut buf).unwrap()
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

        let mut buf = Vec::with_capacity(record_count * 262);

        b.iter(|| {
            for utvec in &data {
                utvec.to_line_protocol(&mut buf).unwrap();
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

        let mut buf: Vec<u8> = Vec::with_capacity(record_count * 262);

        b.iter(|| {
            for utvec in &data {
                utvec.to_line_protocol(&mut buf).unwrap();
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

        let mut buf = Vec::with_capacity(record_count * 262);

        b.iter(|| {
            for utvec in &data {
                utvec.to_line_protocol(&mut buf).unwrap();
            }
            buf.clear();
        })
    });
}

fn ut_vec_write_buf_noloop_benchmark(c: &mut Criterion) {
    c.bench_function("serialize for std::str::from_utf8_unchecked (one record serialization) influxdb", |b| {
        let record : [u8; 257] = [0, 0, 0, 2, 106, 0, 186, 149, 235, 179, 11, 86, 118, 105, 97, 115, 97, 116, 45, 97, 98, 45, 118, 110, 111, 45, 112, 109, 46, 117, 116, 46, 112, 100, 102, 46, 102, 108, 45, 115, 100, 117, 45, 109, 97, 114, 107, 101, 100, 45, 99, 111, 117, 110, 116, 0, 0, 2, 22, 10, 97, 110, 45, 105, 100, 2, 49, 10, 112, 100, 102, 105, 100, 8, 49, 48, 53, 50, 16, 115, 109, 97, 99, 100, 45, 105, 100, 6, 49, 52, 55, 24, 115, 97, 116, 101, 108, 108, 105, 116, 101, 45, 105, 100, 2, 52, 34, 115, 109, 97, 99, 45, 115, 101, 114, 118, 105, 99, 101, 45, 110, 97, 109, 101, 26, 115, 109, 97, 99, 45, 99, 104, 105, 48, 55, 45, 115, 50, 16, 109, 97, 99, 45, 97, 100, 100, 114, 24, 48, 48, 97, 48, 98, 99, 56, 99, 55, 57, 55, 102, 10, 115, 116, 97, 116, 101, 14, 111, 110, 95, 108, 105, 110, 101, 14, 98, 101, 97, 109, 45, 105, 100, 10, 49, 49, 48, 52, 53, 22, 99, 97, 114, 114, 105, 101, 114, 100, 45, 105, 100, 2, 55, 12, 118, 110, 111, 45, 105, 100, 6, 120, 99, 105, 44, 115, 101, 114, 118, 105, 110, 103, 45, 115, 109, 97, 99, 45, 104, 111, 115, 116, 45, 110, 97, 109, 101, 36, 115, 109, 97, 99, 45, 99, 104, 105, 48, 55, 45, 110, 50, 45, 98, 101, 116, 97, 0, 0];
        let visitor = Schema::from_str(SCHEMA_STR).unwrap();
        let mut deserializer = AvroDeserializer{buf: &record[..], schema: &visitor, current_field_index: None };
        deserializer.skip(5);
        let utvec = UTVec::deserialize(&mut deserializer).unwrap();
        let record_count = 10;

        let mut buf = Vec::with_capacity(record_count * 262);

        b.iter(|| {
            utvec.to_line_protocol(&mut buf).unwrap();
            buf.clear();
        })

    });
}

fn ut_vec_write_buf_noloop_nodebug_benchmark(c: &mut Criterion) {
    c.bench_function("serialize for std::str::from_utf8_unchecked (one record serialization, no debug print) influxdb", |b| {
        let record : [u8; 257] = [0, 0, 0, 2, 106, 0, 186, 149, 235, 179, 11, 86, 118, 105, 97, 115, 97, 116, 45, 97, 98, 45, 118, 110, 111, 45, 112, 109, 46, 117, 116, 46, 112, 100, 102, 46, 102, 108, 45, 115, 100, 117, 45, 109, 97, 114, 107, 101, 100, 45, 99, 111, 117, 110, 116, 0, 0, 2, 22, 10, 97, 110, 45, 105, 100, 2, 49, 10, 112, 100, 102, 105, 100, 8, 49, 48, 53, 50, 16, 115, 109, 97, 99, 100, 45, 105, 100, 6, 49, 52, 55, 24, 115, 97, 116, 101, 108, 108, 105, 116, 101, 45, 105, 100, 2, 52, 34, 115, 109, 97, 99, 45, 115, 101, 114, 118, 105, 99, 101, 45, 110, 97, 109, 101, 26, 115, 109, 97, 99, 45, 99, 104, 105, 48, 55, 45, 115, 50, 16, 109, 97, 99, 45, 97, 100, 100, 114, 24, 48, 48, 97, 48, 98, 99, 56, 99, 55, 57, 55, 102, 10, 115, 116, 97, 116, 101, 14, 111, 110, 95, 108, 105, 110, 101, 14, 98, 101, 97, 109, 45, 105, 100, 10, 49, 49, 48, 52, 53, 22, 99, 97, 114, 114, 105, 101, 114, 100, 45, 105, 100, 2, 55, 12, 118, 110, 111, 45, 105, 100, 6, 120, 99, 105, 44, 115, 101, 114, 118, 105, 110, 103, 45, 115, 109, 97, 99, 45, 104, 111, 115, 116, 45, 110, 97, 109, 101, 36, 115, 109, 97, 99, 45, 99, 104, 105, 48, 55, 45, 110, 50, 45, 98, 101, 116, 97, 0, 0];
        let visitor = Schema::from_str(SCHEMA_STR).unwrap();
        let mut deserializer = AvroDeserializer{buf: &record[..], schema: &visitor, current_field_index: None };
        deserializer.skip(5);
        let utvec = UTVec::deserialize(&mut deserializer).unwrap();
        let record_count = 10;

        let mut buf = Vec::with_capacity(record_count * 262);

        b.iter(|| {
            utvec.to_line_protocol(&mut buf).unwrap();
            buf.clear();
        })
    });
}

criterion_group!(benches, ut_deserializer_benchmark, ut_vec_string_conversion_benchmark, ut_vec_raw_string_conversion_benchmark, ut_vec_raw_buf_conversion_benchmark, ut_vec_raw_buf_forloop2_conversion_benchmark, ut_vec_write_buf_noloop_benchmark, ut_vec_write_buf_noloop_nodebug_benchmark);
criterion_main!(benches);
