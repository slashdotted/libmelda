// Melda - Delta State JSON CRDT
// Copyright (C) 2021-2022 Amos Brocco <amos.brocco@supsi.ch>
//
// This program is free software: you can redistribute it and/or modify
// it under the terms of the GNU General Public License as published by
// the Free Software Foundation, either version 3 of the License, or
// (at your option) any later version.
//
// This program is distributed in the hope that it will be useful,
// but WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
// GNU General Public License for more details.
//
// You should have received a copy of the GNU General Public License
// along with this program.  If not, see <http://www.gnu.org/licenses/>.
use crate::adapter::Adapter;
use anyhow::Result;
use base64::{engine::general_purpose, Engine as _};
use std::{cell::RefCell, sync::Mutex};

/// Implements storage in a SQLite database
pub struct SqliteAdapter {
    cn: Mutex<RefCell<rusqlite::Connection>>,
}

impl SqliteAdapter {
    /// Creates a new adapter to store data in a SQLite database (on disk).
    ///
    /// # Arguments
    ///
    /// * `name` - Database name  
    pub fn new(name: &str) -> Self {
        let bk = SqliteAdapter {
            cn: Mutex::new(RefCell::new(rusqlite::Connection::open(name).unwrap())),
        };
        bk.cn
            .lock()
            .unwrap()
            .borrow()
            .execute(
                "CREATE TABLE entries (key VARCHAR NOT NULL PRIMARY KEY, value VARCHAR NOT NULL)",
                [],
            )
            .unwrap();
        bk
    }

    /// Creates a new adapter to store data in a in-memory SQLite database.
    ///
    pub fn new_in_memory() -> Self {
        let bk = SqliteAdapter {
            cn: Mutex::new(RefCell::new(
                rusqlite::Connection::open_in_memory().unwrap(),
            )),
        };
        bk.cn
            .lock()
            .unwrap()
            .borrow()
            .execute(
                "CREATE TABLE entries (key VARCHAR NOT NULL PRIMARY KEY, value VARCHAR NOT NULL)",
                [],
            )
            .unwrap();
        bk
    }
}

impl Adapter for SqliteAdapter {
    /// Reads an object or a sub-object from the backend storage. When offset and length are both 0
    /// the full object is returned, otherwise the sub-object is returned
    ///
    /// # Arguments
    ///
    /// * `key` - The key associated with the object
    /// * `offset` - The starting position of the sub-object in the associated data pack
    /// * `length` - The length of the sub-object (in bytes) in the associated data pack
    ///     
    fn read_object(&self, key: &str, offset: usize, length: usize) -> Result<Vec<u8>> {
        let mcn = self.cn.lock().unwrap();
        let cn = mcn.borrow();
        let mut stmt = cn
            .prepare("SELECT value FROM entries WHERE key = ?1")
            .unwrap();
        let result = stmt.query_row([&key], |row| {
            let data: String = row.get(0)?;
            let data = general_purpose::STANDARD
                .decode(data)
                .expect("cannot_decode_data");
            if offset == 0 && length == 0 {
                Ok(data)
            } else {
                Ok(data.as_slice()[offset..offset + length].to_vec())
            }
        });
        match result {
            Ok(r) => Ok(r),
            Err(_) => Err(anyhow::anyhow!("cannot_read_object")),
        }
    }

    /// Writes an object to the storage
    ///
    /// # Arguments
    ///
    /// * `key` - The key associated with the object
    /// * `data` - The content of the object    
    fn write_object(&self, key: &str, data: &[u8]) -> Result<()> {
        let mcn = self.cn.lock().unwrap();
        let cn = mcn.borrow_mut();
        let value = general_purpose::STANDARD.encode(data);
        match cn.execute(
            "INSERT OR IGNORE INTO entries (key, value) VALUES (?1,?2)",
            [&key, &value.as_str()],
        ) {
            Ok(_) => Ok(()),
            Err(_) => Err(anyhow::anyhow!("cannot_write_object")),
        }
    }

    /// Lists the keys of all objects whose key ends with ext. If ext is an empty string, all objects are returned.
    ///
    /// # Arguments
    ///
    /// * `ext` - The extension (last part of the string) of the requested objects     
    fn list_objects(&self, ext: &str) -> Result<Vec<String>> {
        let mcn = self.cn.lock().unwrap();
        let cn = mcn.borrow();
        let mut stmt = cn.prepare("SELECT key FROM entries")?;
        let rows = stmt.query_map([], |row| row.get(0))?;
        Ok(rows
            .into_iter()
            .filter_map(|key| {
                let key: String = key.unwrap();
                if key.ends_with(ext) {
                    Some(key)
                } else {
                    None
                }
            })
            .collect())
    }
}

#[cfg(test)]
mod tests {
    use crate::{adapter::Adapter, flate2adapter::Flate2Adapter};

    use super::SqliteAdapter;

    #[test]
    fn test_sqlite_read_object_flate() {
        let sa = SqliteAdapter::new_in_memory();
        let ma: Box<dyn Adapter> = Box::new(sa);
        let sqa = Flate2Adapter::new(std::sync::Arc::new(std::sync::RwLock::new(ma)));
        assert!(sqa.list_objects(".delta").unwrap().is_empty());
        assert!(sqa
            .write_object("somekey.delta", "somedata".as_bytes())
            .is_ok());
        assert!(sqa.list_objects(".delta").unwrap().len() == 1);
        let ro = sqa.read_object("somekey.delta", 0, 0);
        assert!(ro.is_ok());
        let ro = ro.unwrap();
        assert!(!ro.is_empty());
        let ro = String::from_utf8(ro).unwrap();
        assert!(ro == "somedata");
        let ro = sqa.read_object("somekey.delta", 1, 2);
        assert!(ro.is_ok());
        let ro = ro.unwrap();
        assert!(!ro.is_empty());
        let ro = String::from_utf8(ro).unwrap();
        assert!(ro == "om");
    }

    #[test]
    fn test_solid_write_object_flate() {
        let sa = SqliteAdapter::new_in_memory();
        let ma: Box<dyn Adapter> = Box::new(sa);
        let sqa = Flate2Adapter::new(std::sync::Arc::new(std::sync::RwLock::new(ma)));
        assert!(sqa.list_objects(".delta").unwrap().is_empty());
        assert!(sqa
            .write_object("somekey.delta", "somedata".as_bytes())
            .is_ok());
        assert!(sqa.list_objects(".delta").unwrap().len() == 1);
        let ro = sqa.read_object("somekey.delta", 0, 0);
        assert!(ro.is_ok());
        let ro = ro.unwrap();
        assert!(!ro.is_empty());
        let ro = String::from_utf8(ro).unwrap();
        assert!(ro == "somedata");
        // Add some other data
        assert!(sqa
            .write_object("somekey.pack", "otherdata".as_bytes())
            .is_ok());
        assert!(sqa.list_objects(".delta").unwrap().len() == 1);
        assert!(sqa.list_objects(".pack").unwrap().len() == 1);
        assert!(sqa.list_objects("").unwrap().len() == 2);
        let ro = sqa.read_object("somekey.pack", 0, 0);
        assert!(ro.is_ok());
        let ro = ro.unwrap();
        assert!(!ro.is_empty());
        let ro = String::from_utf8(ro).unwrap();
        assert!(ro == "otherdata");
        // Do not overwrite if already existing
        assert!(sqa
            .write_object("somekey.pack", "updateddata".as_bytes())
            .is_ok());
        assert!(sqa.list_objects(".delta").unwrap().len() == 1);
        assert!(sqa.list_objects(".pack").unwrap().len() == 1);
        assert!(sqa.list_objects("").unwrap().len() == 2);
        let ro = sqa.read_object("somekey.pack", 0, 0);
        assert!(ro.is_ok());
        let ro = ro.unwrap();
        assert!(!ro.is_empty());
        let ro = String::from_utf8(ro).unwrap();
        assert!(ro == "otherdata");
    }

    #[test]
    fn test_sqlite_list_objects_flate() {
        let sa = SqliteAdapter::new_in_memory();
        let ma: Box<dyn Adapter> = Box::new(sa);
        let sqa = Flate2Adapter::new(std::sync::Arc::new(std::sync::RwLock::new(ma)));
        assert!(sqa.list_objects(".delta").unwrap().is_empty());
        assert!(sqa
            .write_object("somekey.delta", "somedata".as_bytes())
            .is_ok());
        assert!(sqa.list_objects(".delta").unwrap().len() == 1);
        assert!(sqa
            .write_object("somekey.pack", "otherdata".as_bytes())
            .is_ok());
        assert!(sqa.list_objects(".delta").unwrap().len() == 1);
        assert!(sqa.list_objects(".pack").unwrap().len() == 1);
        assert!(sqa.list_objects("").unwrap().len() == 2);
        assert!(sqa
            .write_object("somekey.delta", "somedata".as_bytes())
            .is_ok());
        assert!(sqa.list_objects(".delta").unwrap().len() == 1);
        assert!(sqa
            .write_object("somekey.pack", "otherdata".as_bytes())
            .is_ok());
        assert!(sqa.list_objects(".delta").unwrap().len() == 1);
        assert!(sqa.list_objects(".pack").unwrap().len() == 1);
        assert!(sqa.list_objects("").unwrap().len() == 2);
    }

    #[test]
    fn test_sqlite_read_object() {
        let sqa = SqliteAdapter::new_in_memory();
        assert!(sqa.list_objects(".delta").unwrap().is_empty());
        assert!(sqa
            .write_object("somekey.delta", "somedata".as_bytes())
            .is_ok());
        assert!(sqa.list_objects(".delta").unwrap().len() == 1);
        let ro = sqa.read_object("somekey.delta", 0, 0);
        assert!(ro.is_ok());
        let ro = ro.unwrap();
        assert!(!ro.is_empty());
        let ro = String::from_utf8(ro).unwrap();
        assert!(ro == "somedata");
        let ro = sqa.read_object("somekey.delta", 1, 2);
        assert!(ro.is_ok());
        let ro = ro.unwrap();
        assert!(!ro.is_empty());
        let ro = String::from_utf8(ro).unwrap();
        assert!(ro == "om");
    }

    #[test]
    fn test_sqlite_write_object() {
        let sqa = SqliteAdapter::new_in_memory();
        assert!(sqa.list_objects(".delta").unwrap().is_empty());
        assert!(sqa
            .write_object("somekey.delta", "somedata".as_bytes())
            .is_ok());
        assert!(sqa.list_objects(".delta").unwrap().len() == 1);
        let ro = sqa.read_object("somekey.delta", 0, 0);
        assert!(ro.is_ok());
        let ro = ro.unwrap();
        assert!(!ro.is_empty());
        let ro = String::from_utf8(ro).unwrap();
        assert!(ro == "somedata");
        // Add some other data
        assert!(sqa
            .write_object("somekey.pack", "otherdata".as_bytes())
            .is_ok());
        assert!(sqa.list_objects(".delta").unwrap().len() == 1);
        assert!(sqa.list_objects(".pack").unwrap().len() == 1);
        assert!(sqa.list_objects("").unwrap().len() == 2);
        let ro = sqa.read_object("somekey.pack", 0, 0);
        assert!(ro.is_ok());
        let ro = ro.unwrap();
        assert!(!ro.is_empty());
        let ro = String::from_utf8(ro).unwrap();
        assert!(ro == "otherdata");
        // Do not overwrite if already existing
        assert!(sqa
            .write_object("somekey.pack", "updateddata".as_bytes())
            .is_ok());
        assert!(sqa.list_objects(".delta").unwrap().len() == 1);
        assert!(sqa.list_objects(".pack").unwrap().len() == 1);
        assert!(sqa.list_objects("").unwrap().len() == 2);
        let ro = sqa.read_object("somekey.pack", 0, 0);
        assert!(ro.is_ok());
        let ro = ro.unwrap();
        assert!(!ro.is_empty());
        let ro = String::from_utf8(ro).unwrap();
        assert!(ro == "otherdata");
    }

    #[test]
    fn test_sqlite_list_objects() {
        let sqa = SqliteAdapter::new_in_memory();
        assert!(sqa.list_objects(".delta").unwrap().is_empty());
        assert!(sqa
            .write_object("somekey.delta", "somedata".as_bytes())
            .is_ok());
        assert!(sqa.list_objects(".delta").unwrap().len() == 1);
        assert!(sqa
            .write_object("somekey.pack", "otherdata".as_bytes())
            .is_ok());
        assert!(sqa.list_objects(".delta").unwrap().len() == 1);
        assert!(sqa.list_objects(".pack").unwrap().len() == 1);
        assert!(sqa.list_objects("").unwrap().len() == 2);
        assert!(sqa
            .write_object("somekey.delta", "somedata".as_bytes())
            .is_ok());
        assert!(sqa.list_objects(".delta").unwrap().len() == 1);
        assert!(sqa
            .write_object("somekey.pack", "otherdata".as_bytes())
            .is_ok());
        assert!(sqa.list_objects(".delta").unwrap().len() == 1);
        assert!(sqa.list_objects(".pack").unwrap().len() == 1);
        assert!(sqa.list_objects("").unwrap().len() == 2);
    }
}
