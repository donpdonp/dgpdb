use serde::{Deserialize, Serialize};
use serde_json::Value;
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
    pub fn get(&self, noun: &String) -> Option<&Schema> {
        self.schemas.get(noun)
    }
    pub fn db_name(&self, noun: &String, index_name: &String) -> String {
        if self.schemas.contains_key(noun) {
            noun.to_lowercase() + "." + index_name
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
    pub fn get_key(&self, value: &serde_json::Map<String, Value>) -> Vec<u8> {
        let mut key = String::new();
        for field in &self.fields {
            if let Some(fv) = value.get(field) {
                key.push_str(fv.as_str().unwrap())
            } else {
                println!("warning: field {} is missing from {}", field, self.name)
            }
        }
        return key.as_bytes().to_vec();
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
