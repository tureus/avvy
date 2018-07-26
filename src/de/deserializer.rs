use serde::de::{ Deserializer, Visitor };

use super::super::*;

use byteorder::{ LittleEndian, ReadBytesExt };

pub struct AvroDeserializer<'de> {
    pub buf: &'de [u8],
    pub schema: &'de Schema,
    pub current_field_index: Option<usize>
}

impl<'de, 'a> Deserializer<'de> for &'a mut AvroDeserializer<'de> {
    type Error = AvroError;

    fn deserialize_any<V>(mut self, visitor: V) -> Result<V::Value,Self::Error>
        where V: Visitor<'de> {
        unimplemented!("we don't do free form deserialization")
    }

    fn deserialize_i8<V>(mut self, visitor: V) -> Result<V::Value,Self::Error> where V: Visitor<'de> {
        unimplemented!()
    }

    fn deserialize_i16<V>(mut self, visitor: V) -> Result<V::Value,Self::Error> where V: Visitor<'de> {
        unimplemented!()
    }

    fn deserialize_i32<V>(mut self, visitor: V) -> Result<V::Value,Self::Error> where V: Visitor<'de> {
        info!("deserialize_i32");
        visitor.visit_i32(self.visit_i32())
    }

    fn deserialize_i64<V>(mut self, visitor: V) -> Result<V::Value,Self::Error>
        where V: Visitor<'de> {
        info!("deserialize_i64");
        visitor.visit_i64(self.visit_i64())
    }

    fn deserialize_u8<V>(mut self, visitor: V) -> Result<V::Value,Self::Error> where V: Visitor<'de> {
        unimplemented!()
    }

    fn deserialize_u16<V>(mut self, visitor: V) -> Result<V::Value,Self::Error> where V: Visitor<'de> {
        unimplemented!()
    }

    fn deserialize_u32<V>(mut self, visitor: V) -> Result<V::Value,Self::Error> where V: Visitor<'de> {
        visitor.visit_u32(self.visit_u32())
    }

    fn deserialize_u64<V>(mut self, visitor: V) -> Result<V::Value,Self::Error> where V: Visitor<'de> {
        visitor.visit_u64(self.visit_u64())
    }

    fn deserialize_f32<V>(mut self, visitor: V) -> Result<V::Value,Self::Error> where V: Visitor<'de> {
        let val = self.buf.read_f32::<LittleEndian>().unwrap();
        self.buf = &self.buf[4..];
        info!("deserialize_f32: {}", val);
        visitor.visit_f32(val)
    }

    fn deserialize_f64<V>(mut self, visitor: V) -> Result<V::Value,Self::Error> where V: Visitor<'de> {
        let val = self.visit_f64();
        visitor.visit_f64(val)
    }


    fn deserialize_string<V>(mut self, visitor: V) -> Result<V::Value, Self::Error>
        where V: Visitor<'de> {
        info!("deserialize string...");
        let string = String::from_utf8(self.visit_str().to_owned()).unwrap();
        visitor.visit_string(string)
    }

    fn deserialize_bytes<V>(mut self, visitor: V) -> Result<V::Value, Self::Error>
        where V: Visitor<'de> {
        info!("deserialize bytes...");
        let string = self.visit_str();
        visitor.visit_borrowed_bytes(string)
    }

    fn deserialize_str<V>(mut self, visitor: V) -> Result<V::Value, Self::Error>
        where V: Visitor<'de> {
        info!("deserialize bytes...");
        let string = self.visit_str();
        visitor.visit_borrowed_str(unsafe{ std::str::from_utf8_unchecked(string) })
    }


    fn deserialize_option<V>(mut self, visitor: V) -> Result<V::Value, Self::Error>
        where V: Visitor<'de> {
        info!("deserialize option...");
        let enum_variant = {
            self.visit_int() as usize
        };

//        info!("option variant: {}", enum_variant);

        let current_field = self.current_field();
        if current_field.types.len() != 2 {
            return Err(AvroError{ reason: "this should be an option but the schema's union is too small".into() })
        } else {
            if enum_variant >= current_field.types.len() {
//                self.dump();
                return Err(AvroError{reason: format!("option variant id for {} is out of scope, got {} but max is {}", current_field.name, enum_variant, current_field.types.len())})
            } else if current_field.types[enum_variant] == SchemaFieldType::Primitive(Primitive::Null) {
                info!("option is a None");
                visitor.visit_none()
            } else {
                info!("option is Some");
                visitor.visit_some(self)
            }
        }
    }

    fn deserialize_map<V>(mut self, visitor: V) -> Result<V::Value,Self::Error>
        where V: Visitor<'de> {
        let mut size = self.visit_long();
        if size < 0 {
            size *= -1;
        }

        let map_visitor = AvroValueMapAccess{de: &mut self, blocks: size, entries: size*2};
        info!("deserialize_map entries: {} => {}", size, size*2);
        visitor.visit_map( map_visitor)
    }

    fn deserialize_struct<V>(mut self, _id: &'static str, fields: &'static[&'static str], visitor: V) -> Result<V::Value,Self::Error>
        where V: Visitor<'de> {
        info!("deserialize_struct -> map visitor");

        visitor.visit_map(AvroIdentifierMapVisitor {de: &mut self, count: 0, expected: fields.len()})
    }

    fn deserialize_enum<V>(mut self, enum_name: &'static str, enum_variants: &[&'static str], visitor: V) -> Result<V::Value, Self::Error>
        where V: Visitor<'de> {
        let field = self.current_field();
        info!("deserialize_enum enum_name: {}, enum_variants: {:?}", enum_name, enum_variants);

        let value = visitor.visit_enum(AvroEnumVisitor::new(self, enum_name, enum_variants) )?;
        Ok(value)
    }

    fn deserialize_identifier<V>(mut self, visitor: V) -> Result<V::Value, Self::Error>
        where V: Visitor<'de> {
        self.next_field();
        let current_field = self.current_field();
        info!("deserialize_identifier {}", current_field.name);

        let res = visitor.visit_string(current_field.name.clone());
        res
    }

    fn deserialize_seq<V>(mut self, visitor: V) -> Result<V::Value, Self::Error>
        where V: Visitor<'de> {

        visitor.visit_seq(super::AvroSeqVisitor::new( &mut self))
    }

    fn deserialize_tuple<V>(mut self, size: usize, visitor: V) -> Result<V::Value, Self::Error>
        where V: Visitor<'de> {

        visitor.visit_seq(super::AvroTupleVisitor::new( &mut self, size))
    }

    forward_to_deserialize_any!{
        <V: Visitor<'de>>
        bool char byte_buf unit unit_struct newtype_struct tuple_struct ignored_any
    }
}

impl<'de> AvroDeserializer<'de> {
    pub fn dump(&self) {
        error!("dumping: {:?}", self.buf);
    }

    pub fn skip(&mut self, bytes: usize) {
        self.buf = &self.buf[bytes..];
    }

    pub fn peek(&self) -> u8 {
        self.buf[0]
    }

    pub fn visit_u32(&mut self) -> u32 {
        let (val,size) = integer_encoding::VarInt::decode_var(self.buf);
        info!("visit_u32 val: {}, size: {}", val, size);

        self.buf = &self.buf[size..];
        val
    }

    pub fn visit_u64(&mut self) -> u64 {
        let (val,size) = integer_encoding::VarInt::decode_var(self.buf);
        info!("val: {}, size: {}", val, size);

        self.buf = &self.buf[size..];
        val
    }

    pub fn visit_i32(&mut self) -> i32 {
        let (val,size) = integer_encoding::VarInt::decode_var(self.buf);
        info!("val: {}, size: {}", val, size);

        self.buf = &self.buf[size..];
        val
    }

    pub fn visit_i64(&mut self) -> i64 {
        let (val,varsize) : (i64,usize) = integer_encoding::VarInt::decode_var(self.buf);
        info!("visit_i64 val2: {}, varsize: {}", val, varsize);

        self.buf = &self.buf[varsize..];
        val
    }

    pub fn visit_f32(&mut self) -> f32 {
        let val = self.buf.read_f32::<LittleEndian>().unwrap();
        info!("deserialize_f32: {}", val);

        self.buf = &self.buf[8..];
        val
    }

    pub fn visit_f64(&mut self) -> f64 {
        let val = self.buf.read_f64::<LittleEndian>().unwrap();
        info!("deserialize_f64: {}", val);

        self.buf = &self.buf[8..];
        val
    }

    pub fn visit_uint(&mut self) -> u32 {
        let (int,varsize) = integer_encoding::VarInt::decode_var(self.buf);
        self.buf = &self.buf[varsize..];
        int
    }

    pub fn visit_int(&mut self) -> i32 {
        let (int,varsize) = integer_encoding::VarInt::decode_var(self.buf);
        self.buf = &self.buf[varsize..];
        info!("visit_int: {}, size: {}", int, varsize);
        int
    }

    pub fn visit_long(&mut self) -> i64 {
        let (int,varsize) = integer_encoding::VarInt::decode_var(self.buf);
        self.buf = &self.buf[varsize..];
        info!("visit_long: {}, size: {}", int, varsize);
        int
    }

    pub fn visit_str(&mut self) -> &'de [u8] {
        let strlen = self.visit_long();
        info!("strlen: {}", strlen);

        let rstr = &self.buf[..strlen as usize];
        info!("rstr: {}", String::from_utf8(rstr.to_owned()).unwrap());

        self.buf = &self.buf[strlen as usize..];

        rstr
    }
}

impl<'de> AvroDeserializer<'de> {
    pub fn from_slice(schema: &'de Schema, buf: &'de [u8]) -> Self {
        AvroDeserializer {
            buf,
            schema,
            current_field_index: None,
        }
    }

    fn next_field(&mut self) {
        if let Some(ref mut cfi) = self.current_field_index {
            *cfi += 1;
        } else {
            self.current_field_index = Some(0);
        };
        info!("done with field, now on current_field_index {:?}", self.current_field_index);
    }

    fn current_field(&self) -> &SchemaField {
        debug!("current_field index: {:?}", self.current_field_index);
        &self.schema.fields[self.current_field_index.unwrap()]
    }
}