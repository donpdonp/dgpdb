use lmdb;
use lmdb::{Cursor, Transaction};
use std::path::Path;

use protobuf;
use crate::schema;

#[derive(Debug)]
pub struct Db {
    pub env: lmdb::Environment,
    pub schemas: schema::Schemas,
    pub file_path: String,
}

pub fn open() -> Db {
    let data_dir = "lmdb-data";
    ensure_dir(data_dir).unwrap();
    let json_dir = "jsonlake";
    ensure_dir(json_dir).unwrap();
    let schema_json = "schema.json";
    let schemas = schema::from_file(schema_json);
    let schemas_count: u32 = schemas.len().try_into().unwrap();
    let env = lmdb::Environment::new()
        .set_max_dbs(schemas_count + 1)
        .open(Path::new(data_dir))
        .unwrap();
    return Db {
        env: env,
        schemas: schemas,
        file_path: json_dir.to_owned(),
    };
}

pub fn ensure_dir(dir: &str) -> Result<(), std::io::Error> {
    std::fs::create_dir_all(dir)
}

pub fn name_value<T: protobuf::MessageFull>() -> String {
    T::descriptor().name().to_string()
}

pub fn id_value<T: protobuf::MessageFull>(value: &T) -> String {
    let desc = T::descriptor();
    let answer = desc.fields().find(|field| field.name() == "id").unwrap();
    answer.get_singular(value).unwrap().to_str().unwrap().to_string()
}

impl Db {
    pub fn file_from_id(&self, id: &String) -> String {
        format!("{}/{}", self.file_path, id)
    }

    pub fn write<T: protobuf::MessageFull> (&self, value: &T) -> String {
        let noun_name = name_value::<T>();
        let id = id_value(value);
        let schema = self.schemas.get(&noun_name);
        if let Some(sch) = schema {
            for index in sch.indexes.iter() {
                let idx_db_name = self.schemas.db_name(&noun_name, &index.name);
                let index_db = self
                    .env
                    .create_db(Some(&idx_db_name), lmdb::DatabaseFlags::empty())
                    .unwrap();
                let mut tx = self.env.begin_rw_txn().unwrap();
                let key = index.get_key(value);
                let result = tx.get(index_db, &key);
                match result {
                    Err(_) => match noun_name.as_str() {
                        "location" => {
                            println!(
                                "writing {} key:{} value: {}",
                                idx_db_name,
                                String::from_utf8_lossy(&key),
                                id
                            );
                            tx.put(index_db, &key, &id, lmdb::WriteFlags::empty())
                                .unwrap()
                        }
                        _ => ()
                    },
                    Ok(v) => println!(
                        "exists: {} {:?}: {:?}",
                        idx_db_name,
                        String::from_utf8_lossy(&key),
                        String::from_utf8_lossy(v)
                    ),
                }
                tx.commit().unwrap();
                self.dump(&idx_db_name);
            }
        }
        id
    }

    pub fn dump(&self, name: &str) {
        println!("---db dump {} ---", name);
        let ddb = self.env.open_db(Some(&name)).unwrap();
        let ro = self.env.begin_ro_txn().unwrap();
        {
            let mut c = ro.open_ro_cursor(ddb).unwrap();
            let mut count = 0;
            for kv in c.iter() {
                count += count;
                let k = String::from_utf8_lossy(kv.0);
                let v = String::from_utf8_lossy(kv.1);
                println!("{} {:?} {:?}", count, k, v);
            }
        }
    }
}
