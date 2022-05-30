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
use std::{cell::RefCell, sync::Mutex};

pub struct SqliteAdapter {
    cn: Mutex<RefCell<rusqlite::Connection>>,
}

impl SqliteAdapter {
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
    fn read_object(&self, key: &str, offset: usize, length: usize) -> Result<Vec<u8>> {
        let mcn = self.cn.lock().unwrap();
        let cn = mcn.borrow();
        let mut stmt = cn
            .prepare("SELECT value FROM entries WHERE key = ?1")
            .unwrap();
        let result = stmt.query_row(&[&key], |row| {
            let data: String = row.get(0)?;
            let data: Vec<u8> = data.as_bytes().iter().map(|c| *c as u8).collect::<Vec<_>>();
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

    fn write_object(&self, key: &str, data: &[u8]) -> Result<()> {
        let mcn = self.cn.lock().unwrap();
        let cn = mcn.borrow_mut();
        let value = String::from_utf8(data.to_vec()).unwrap();
        match cn.execute(
            "INSERT OR IGNORE INTO entries (key, value) VALUES (?1,?2)",
            &[&key, &value.as_str()],
        ) {
            Ok(_) => Ok(()),
            Err(_) => Err(anyhow::anyhow!("cannot_write_object")),
        }
    }

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
    use crate::adapter::Adapter;

    use super::SqliteAdapter;

    #[test]
    fn test_read_object() {
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
    fn test_write_object() {
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
    fn test_list_objects() {
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
