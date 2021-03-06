use lmdb;
use lmdb::{Cursor, Transaction};
use std::path::Path;

use crate::schema;
use protobuf;

#[derive(Debug)]
pub struct Db {
    pub env: lmdb::Environment,
    pub schemas: schema::Schemas,
    pub file_path: String,
}

pub fn open(schema_file: String) -> Db {
    let data_dir = "lmdb-data";
    ensure_dir(data_dir).unwrap();
    let json_dir = "jsonlake";
    ensure_dir(json_dir).unwrap();
    let schemas = schema::from_file(&schema_file);
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
    T::descriptor().name().to_string().to_lowercase()
}

pub fn id_value<T: protobuf::MessageFull>(value: &T) -> String {
    let desc = T::descriptor();
    let answer = desc.fields().find(|field| field.name() == "id").unwrap();
    answer
        .get_singular(value)
        .unwrap()
        .to_str()
        .unwrap()
        .to_string()
}

pub fn id_new<T: protobuf::MessageFull>(_value: &T) -> String {
    let id = ulid::Ulid::new().to_string();
    let noun_name_bytes = name_value::<T>().to_lowercase().into_bytes();
    // 6 bytes time, 10 bytes rnd
    let mut id_bytes = id.into_bytes();
    id_bytes[6] = noun_name_bytes[0];
    id_bytes[7] = noun_name_bytes[1];
    String::from_utf8(id_bytes).unwrap()
}

impl Db {
    pub fn filename_from_id(&self, id: &str) -> String {
        format!("{}/{}", self.file_path, id)
    }

    fn open_db(&self, idx_db_name: &str) -> lmdb::Database {
        self.env
            .create_db(Some(&idx_db_name), lmdb::DatabaseFlags::empty())
            .unwrap()
    }

    pub fn put<T: protobuf::MessageFull>(&self, value: &T) -> String {
        let noun_name = name_value::<T>().to_lowercase();
        let id = id_value(value);
        let schema = self.schemas.get(&noun_name);
        if let Some(sch) = schema {
            for index in sch.indexes.iter() {
                let idx_db_name = self.schemas.db_name(&noun_name, &index.name);
                let index_db = self.open_db(&idx_db_name);
                let mut tx = self.env.begin_rw_txn().unwrap();
                match index.get_key(value) {
                    Ok(key) => {
                        match tx.get(index_db, &key) {
                            Err(_) => {
                                println!(
                                    "writing {} key: {} value: {}",
                                    idx_db_name,
                                    String::from_utf8_lossy(&key),
                                    id
                                );
                                tx.put(index_db, &key, &id, lmdb::WriteFlags::empty())
                                    .unwrap()
                            }
                            Ok(value) => println!(
                                "exists: {} {:?}: {:?}",
                                idx_db_name,
                                String::from_utf8_lossy(&key),
                                String::from_utf8_lossy(value)
                            ),
                        }
                        tx.commit().unwrap();
                        self.dump(&idx_db_name);
                    }
                    Err(msg) => println!("{}", msg),
                };
            }
        } else {
            println!("warning: no schema for {}", noun_name);
        }
        id
    }

    pub fn get(
        &self,
        noun_name: &str,
        index_name: &str,
        key: String,
    ) -> Result<String, crate::Error> {
        let idx_db_name = self.schemas.db_name(noun_name, index_name);
        let index_db = self.open_db(&idx_db_name);
        let tx = self.env.begin_ro_txn().unwrap();
        let result = tx.get(index_db, &key);
        let id = match result {
            Ok(result) => Ok(String::from_utf8_lossy(result).into_owned()),
            Err(_) => Err(crate::Error {}),
        };
        tx.commit().unwrap();
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
