use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::fs::File;
use std::io::BufReader;

#[derive(Serialize, Deserialize, Debug)]
#[serde(transparent)]
pub struct Schemas {
    schemas: HashMap<String, Schema>,
}

impl Schemas {
    pub fn len(&self) -> usize {
        self.schemas.len()
    }
    pub fn get(&self, noun: &str) -> Option<&Schema> {
        self.schemas.get(noun)
    }
    pub fn db_name(&self, noun: &str, index_name: &str) -> String {
        if self.schemas.contains_key(noun) {
            noun.to_owned() + "." + index_name
        } else {
            "error".to_owned()
        }
    }
}

#[derive(Serialize, Deserialize, Debug)]
pub struct Schema {
    pub indexes: Vec<Index>,
}

#[derive(Serialize, Deserialize, Debug)]
pub struct Index {
    pub name: String,
    pub fields: Vec<String>,
    pub options: Options,
}

impl Index {
    pub fn get_key<T: protobuf::MessageFull>(&self, value: &T) -> Result<Vec<u8>, String> {
        println!("index.get_key value {}", value);
        let mut key_parts = Vec::<String>::new();
        for field in &self.fields {
            let descriptor = T::descriptor();
            if let Some(fv) = descriptor.field_by_name(field) {
                println!("index.get_key {} {}", descriptor, fv);
                let value = match fv.get_singular(value){
                    Some(value) => value,
                    None => return Err(format!("index.get_key {} missing", fv))
                };
                key_parts.push(value.to_str().unwrap().to_string());
            } else {
                println!("warning: field {} is missing from {}", field, self.name)
            }
        }
        let key = key_parts.join(":");
        return Ok(key.as_bytes().to_vec());
    }
}

#[derive(Serialize, Deserialize, Debug)]
pub struct Options {
    #[serde(default)]
    multi: bool,
}
pub fn from_file(filename: &str) -> Schemas {
    let file = File::open(filename).unwrap();
    let reader = BufReader::new(file);

    // Read the JSON contents of the file as an instance of `User`.
    let schemas: Schemas = serde_json::from_reader(reader).unwrap();
    for part in &schemas.schemas {
        println!("schema {:?}", part);
    }
    schemas
}
