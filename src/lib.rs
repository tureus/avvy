#![feature(nll, test)]

#[macro_use] extern crate serde;
#[macro_use] extern crate serde_derive;

extern crate byteorder;

#[macro_use] extern crate log;
extern crate test;

extern crate serde_json;

extern crate integer_encoding;

mod schema;
pub use schema::*;

mod de;
pub use de::*;


// Temporary while I'm on the plane!
pub mod cdr;
pub mod buferator;
