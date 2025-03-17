// Melda - Delta State JSON CRDT
// Copyright (C) 2021-2024 Amos Brocco <amos.brocco@supsi.ch>
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
use std::{cell::RefCell, collections::BTreeMap, sync::Mutex};

/// Implements in-memory storage
pub struct MemoryAdapter {
    data: Mutex<RefCell<BTreeMap<String, Vec<u8>>>>,
}

impl MemoryAdapter {
    /// Creates a new adapter to store data in memory
    pub fn new() -> Self {
        MemoryAdapter {
            data: Mutex::new(RefCell::new(BTreeMap::<String, Vec<u8>>::new())),
        }
    }
}

impl Default for MemoryAdapter {
    fn default() -> Self {
        Self::new()
    }
}

impl Adapter for MemoryAdapter {
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
        let mem = self.data.lock().unwrap();
        let d = mem.borrow();
        let data = d.get(key).unwrap();
        if offset == 0 && length == 0 {
            Ok(data.clone())
        } else {
            Ok(data.as_slice()[offset..offset + length].to_vec())
        }
    }

    /// Writes an object to the storage
    ///
    /// # Arguments
    ///
    /// * `key` - The key associated with the object
    /// * `data` - The content of the object    
    fn write_object(&self, key: &str, data: &[u8]) -> Result<()> {
        let mem = self.data.lock().unwrap();
        let mut d = mem.borrow_mut();
        if !d.contains_key(key) {
            d.insert(key.to_string(), data.to_vec());
        }
        Ok(())
    }

    /// Lists the keys of all objects whose key ends with ext. If ext is an empty string, all objects are returned.
    ///
    /// # Arguments
    ///
    /// * `ext` - The extension (last part of the string) of the requested objects     
    fn list_objects(&self, ext: &str) -> Result<Vec<String>> {
        let list: Vec<String> = self
            .data
            .lock()
            .unwrap()
            .borrow()
            .keys()
            .filter(|x| x.ends_with(ext))
            .map(|x| x.strip_suffix(ext).unwrap().to_string())
            .collect();
        Ok(list)
    }
}

#[cfg(test)]
mod tests {
    use crate::{adapter::Adapter, flate2adapter::Flate2Adapter};

    use super::MemoryAdapter;

    #[test]
    fn test_memory_read_object_flate() {
        let sa = MemoryAdapter::new();
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
    fn test_memory_write_object_flate() {
        let sa = MemoryAdapter::new();
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
    fn test_memory_list_objects_flate() {
        let sa = MemoryAdapter::new();
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
    fn test_memory_read_object() {
        let sqa = MemoryAdapter::new();
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
    fn test_memory_write_object() {
        let sqa = MemoryAdapter::new();
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
    fn test_memory_list_objects() {
        let sqa = MemoryAdapter::new();
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
