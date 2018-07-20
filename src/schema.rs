use serde::{self, Deserialize};
use serde_json;

#[derive(Serialize, Deserialize, Debug)]
pub struct Schema {
    #[serde(rename = "type")]
    schema_type: String,
    name: String,
    namespace: String,
    pub fields: Vec<SchemaField>,
}

impl Schema {
    pub fn from_str(schema: &str) -> serde_json::Result<Self> {
        serde_json::from_str(schema)
    }
}

#[derive(Serialize, Deserialize, Debug)]
pub struct SchemaField {
    pub name: String,
    #[serde(rename = "type", deserialize_with = "one_or_many")]
    pub types: Vec<SchemaFieldType>,
}

#[derive(Serialize, Deserialize, Debug, PartialEq)]
#[serde(untagged)]
pub enum SchemaFieldType {
    Primitive(Primitive),
    Complex(Complex),
}

#[derive(Serialize, Deserialize, Debug, PartialEq)]
#[serde(rename_all = "lowercase")]
pub enum Primitive {
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

#[derive(Serialize, Deserialize, Debug, PartialEq)]
#[serde(tag = "type", rename_all = "lowercase")]
pub enum Complex {
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
        D: serde::de::Deserializer<'de>,
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