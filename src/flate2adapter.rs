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
use flate2::{read::DeflateDecoder, write::DeflateEncoder, Compression};
use std::{
    io::{Read, Write},
    sync::{Arc, RwLock},
};

/// Implements compressed storage (using DEFLATE) on other adapters
pub struct Flate2Adapter {
    backend: Arc<RwLock<Box<dyn Adapter>>>,
}

impl Flate2Adapter {
    /// Creates a new adapter wrapping the specified adapter
    ///
    /// # Arguments
    ///
    /// * `backend` - The adapter to be wrapped
    pub fn new(backend: Arc<RwLock<Box<dyn Adapter>>>) -> Self {
        Flate2Adapter { backend }
    }
}

impl Adapter for Flate2Adapter {
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
        let key = key.to_string() + ".flate"; // Change key to avoid mismatching cache objects
        let data = self.backend.read().unwrap().read_object(&key, 0, 0)?;
        let mut d = DeflateDecoder::new(data.as_slice());
        let mut datavec = vec![];
        d.read_to_end(&mut datavec)?;
        if offset == 0 && length == 0 {
            Ok(datavec)
        } else {
            Ok(datavec.as_slice()[offset..offset + length].to_vec())
        }
    }

    /// Writes an object to the storage
    ///
    /// # Arguments
    ///
    /// * `key` - The key associated with the object
    /// * `data` - The content of the object    
    fn write_object(&self, key: &str, data: &[u8]) -> Result<()> {
        let key = key.to_string() + ".flate"; // Change key to avoid mismatching cache objects
        let mut e = DeflateEncoder::new(Vec::new(), Compression::default());
        e.write_all(data)?;
        let compressed = e.finish().unwrap();
        self.backend
            .write()
            .unwrap()
            .write_object(&key, compressed.as_slice())
    }

    /// Lists the keys of all objects whose key ends with ext. If ext is an empty string, all objects are returned.
    ///
    /// # Arguments
    ///
    /// * `ext` - The extension (last part of the string) of the requested objects     
    fn list_objects(&self, ext: &str) -> Result<Vec<String>> {
        let ext = ext.to_string() + ".flate"; // Change key to avoid mismatching cache objects
        let result = self.backend.read().unwrap().list_objects(&ext)?;
        Ok(result
            .into_iter()
            .map(|k| k.trim_end_matches(".flate").to_string())
            .collect())
    }
}

mod tests {
    #[allow(unused_imports)]
    use crate::{adapter::Adapter, flate2adapter::Flate2Adapter, memoryadapter::MemoryAdapter};

    #[test]
    fn test_read_object() {
        let ma: Box<dyn Adapter> = Box::new(MemoryAdapter::new());
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
    fn test_write_object() {
        let ma: Box<dyn Adapter> = Box::new(MemoryAdapter::new());
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
    fn test_list_objects() {
        let ma: Box<dyn Adapter> = Box::new(MemoryAdapter::new());
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
}
