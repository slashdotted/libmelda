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
use flate2::{read::DeflateDecoder, write::DeflateEncoder, Compression};
use std::{
    io::{Read, Write},
    sync::{Arc, RwLock},
};

pub struct Flate2Adapter {
    backend: Arc<RwLock<Box<dyn Adapter>>>,
}

impl Flate2Adapter {
    pub fn new(backend: Arc<RwLock<Box<dyn Adapter>>>) -> Self {
        Flate2Adapter { backend }
    }
}

impl Adapter for Flate2Adapter {
    fn read_object(&self, key: &str, offset: usize, length: usize) -> Result<Vec<u8>> {
        let data = self.backend.read().unwrap().read_object(key, 0, 0)?;
        let mut d = DeflateDecoder::new(data.as_slice());
        let mut datavec = vec![];
        d.read_to_end(&mut datavec)?;
        if offset == 0 && length == 0 {
            Ok(datavec.clone())
        } else {
            Ok(datavec.as_slice()[offset..offset + length].to_vec())
        }
    }

    fn write_object(&self, key: &str, data: &[u8]) -> Result<()> {
        let mut e = DeflateEncoder::new(Vec::new(), Compression::default());
        e.write_all(data)?;
        let compressed = e.finish().unwrap();
        self.backend
            .write()
            .unwrap()
            .write_object(key, compressed.as_slice())
    }

    fn list_objects(&self, ext: &str) -> Result<Vec<String>> {
        self.backend.read().unwrap().list_objects(ext)
    }
}
