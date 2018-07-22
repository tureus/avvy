extern crate env_logger;

extern crate serde;
#[macro_use] extern crate serde_derive;
extern crate avvy;

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
    pub metric: String,
    pub value: Value,
    #[serde(borrow)]
    pub tags: Option<std::collections::HashMap<&'a [u8], &'a [u8]>>,
    #[serde(borrow)]
    pub metadata: Option<std::collections::HashMap<&'a [u8], &'a [u8]>>
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
    let schema = avvy::Schema::from_str(SCHEMA_STR).unwrap();

    for (i,test) in (&tests[..]).iter().enumerate() {
        let buf = &test[..];
        let buf_len = buf.len();

        let mut de = avvy::AvroDeserializer{buf, schema: &schema, current_field_index: None  };
        de.skip(5);
        let ut = UT::deserialize(&mut de);
        match ut {
            Ok(ut) => {
                println!("ut: {:?}", ut)
            },
            Err(e) => {
                let mut de = avvy::AvroDeserializer{buf, schema: &schema, current_field_index: None  };
                de.skip(5);
                let _ut = UTSafe::deserialize(&mut de).unwrap();
                let end = buf_len-de.buf.len();
                println!("error on {}", i);
                println!("parse failed: {} (left off at {})", e, end);
                println!("parse failed: {:?}", buf);
                let good = &buf[0..end];
                println!("good buf: {:?}", good);
                let bad = &buf[end..];
                println!("bad  buf: {:?}", bad);

                println!("ts bytes: {:?}", &buf[5..11]);
                println!("metric bytes: {:?} ({})", &buf[12..12+1+26], unsafe { String::from_utf8_unchecked((&buf[12..12+1+26]).to_owned()) });

                println!("value bytes: {:?}", &buf[39..]);
                println!("value parsed: {:?}", _ut.value);

                println!("tags bytes: {:?}", &buf[39+9..]);

                let tag_buf = &buf[39+9..];
                let mut fde = avvy::AvroDeserializer{buf: tag_buf, schema: &schema, current_field_index: None};

                let variant = fde.visit_int();
                let blocks = fde.visit_int();
                println!("enum variant: {}, blocks: {}", variant, blocks );
                for _b in 0..blocks*2 {
                    let bytes = fde.visit_str();
                    let stringy = String::from_utf8(bytes.to_owned()).unwrap();
                    println!("stringy: {} / bytes: {:?}", stringy, bytes);
                }

                let variant = fde.visit_int();
                let blocks = fde.visit_int();
                println!("enum variant: {}, blocks: {}", variant, blocks );
                for _b in 0..blocks*2 {
                    let bytes = fde.visit_str();
                    let stringy = String::from_utf8(bytes.to_owned()).unwrap();
                    println!("stringy: {}", stringy);
                }

                fde.dump();
            }
        }
    }
}

fn test_data() -> Vec<Vec<u8>> {
    vec![
        vec![0, 0, 0, 2, 106, 0, 132, 221, 148, 181, 11, 54, 118, 105, 97, 115, 97, 116, 45, 97, 98, 45, 118, 110, 111, 45, 112, 109, 46, 117, 116, 46, 114, 108, 45, 115, 105, 110, 114, 6, 72, 225, 122, 20, 174, 71, 11, 64, 2, 20, 10, 97, 110, 45, 105, 100, 2, 49, 16, 115, 109, 97, 99, 100, 45, 105, 100, 6, 49, 51, 50, 24, 115, 97, 116, 101, 108, 108, 105, 116, 101, 45, 105, 100, 2, 52, 34, 115, 109, 97, 99, 45, 115, 101, 114, 118, 105, 99, 101, 45, 110, 97, 109, 101, 26, 115, 109, 97, 99, 45, 114, 101, 115, 48, 56, 45, 115, 50, 16, 109, 97, 99, 45, 97, 100, 100, 114, 24, 48, 48, 97, 48, 98, 99, 56, 49, 50, 56, 54, 52, 10, 115, 116, 97, 116, 101, 14, 111, 110, 95, 108, 105, 110, 101, 12, 118, 110, 111, 45, 105, 100, 16, 101, 120, 101, 100, 101, 114, 101, 115, 44, 115, 101, 114, 118, 105, 110, 103, 45, 115, 109, 97, 99, 45, 104, 111, 115, 116, 45, 110, 97, 109, 101, 38, 115, 109, 97, 99, 45, 114, 101, 115, 48, 56, 45, 110, 50, 45, 97, 108, 112, 104, 97, 14, 98, 101, 97, 109, 45, 105, 100, 10, 49, 48, 48, 56, 54, 22, 99, 97, 114, 114, 105, 101, 114, 100, 45, 105, 100, 4, 49, 51, 0, 0],
    ]
}