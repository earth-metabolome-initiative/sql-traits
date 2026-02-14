//! Implementation of the `SchemaLike` trait for the `Schema` struct.

use crate::{
    structs::{ParserDB, Schema},
    traits::{Metadata, SchemaLike},
};

impl Metadata for Schema {
    type Meta = ();
}

impl SchemaLike for Schema {
    type DB = ParserDB;

    fn name(&self) -> &str {
        Schema::name(self)
    }

    fn authorization(&self) -> Option<&str> {
        Schema::authorization(self)
    }
}
