#![feature(nll, benchmark, test)]

#[macro_use] extern crate serde;
#[macro_use] extern crate serde_derive;

#[macro_use] extern crate log;
extern crate test;

extern crate serde_json;

extern crate integer_encoding;

mod schema;
pub use schema::*;

mod de;
pub use de::*;
