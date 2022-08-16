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
use crate::constants::{HASH_FIELD, INDEX_EXTENSION, PACK_EXTENSION};
use crate::revision::Revision;
use crate::utils::digest_bytes;
use anyhow::{anyhow, bail, Result};
use lru::LruCache;
use serde_json::json;
use serde_json::Map;
use serde_json::Value;
use std::cell::RefCell;
use std::collections::BTreeSet;
use std::collections::{HashMap, HashSet};
use std::sync::{Arc, Mutex, RwLock};

pub struct DataStorage {
    adapter: Arc<RwLock<Box<dyn Adapter>>>,
    stage: HashMap<String, Value>,
    values: HashMap<String, (String, usize, usize)>,
    loaded_packs: BTreeSet<String>,
    cache: Mutex<RefCell<LruCache<String, Map<String, Value>>>>,
    force_full_array_interval: u32,
}



impl DataStorage {
    /// Constructs a new Data storage based on the provided adapter
    pub fn new(adapter: Arc<RwLock<Box<dyn Adapter>>>) -> DataStorage {
        let cache_size = std::env::var("MELDA_DATA_CACHE_CAP")
            .unwrap_or_else(|_| "16".to_string())
            .parse::<u32>()
            .unwrap() as usize;
        let full_array_interval = std::env::var("MELDA_FORCE_FULL_ARRAY_INTERVAL")
            .unwrap_or_else(|_| "1000".to_string())
            .parse::<u32>()
            .unwrap();
        DataStorage {
            adapter,
            stage: HashMap::<String, Value>::new(),
            values: HashMap::<String, (String, usize, usize)>::new(),
            loaded_packs: BTreeSet::new(),
            cache: Mutex::new(RefCell::new(LruCache::<String, Map<String, Value>>::new(
                cache_size,
            ))),
            force_full_array_interval: full_array_interval,
        }
    }

    /// Merges another DataStorage into this one
    pub fn merge(&mut self, other: &DataStorage) -> Result<()> {
        other.values.keys().for_each(|digest| {
            if !self.values.contains_key(digest) && !self.stage.contains_key(digest) {
                self.write_raw_value(
                    digest,
                    other
                        .read_raw_value(digest)
                        .expect("failed_to_read_data")
                )
                .expect("failed_to_write_data");
            }
        });
        other.stage.keys().for_each(|digest| {
            if !self.values.contains_key(digest) && !self.stage.contains_key(digest) {
                self.write_raw_value(
                    digest,
                    other
                        .read_raw_value(digest)
                        .expect("failed_to_read_data")
                )
                .expect("failed_to_write_data");
            }
        });
        Ok(())
    }

    /// Loads a pack file (and rebuilds the index)
    fn load_pack(&mut self, pack: &str) -> Result<()> {
        let object = pack.to_string() + PACK_EXTENSION;
        let data = self
            .adapter
            .read()
            .unwrap()
            .read_object(object.as_str(), 0, 0)?;
        self.load_pack_data(pack, &data)
    }

    /// Data is the raw string (we need to compute the offset and length of the object)
    fn load_pack_data(&mut self, name: &str, data: &[u8]) -> Result<()> {
        let mut flag = 0;
        let mut obj_start = 0;
        for (offset, c) in data.iter().enumerate() {
            if *c == b'{' {
                if flag == 0 {
                    obj_start = offset;
                };
                flag += 1;
            } else if *c == b'}' {
                flag -= 1;
                if flag == 0 {
                    let digest = digest_bytes(&data[obj_start..offset + 1]);
                    let count = offset + 1 - obj_start;
                    self.values
                        .insert(digest, (name.to_string(), obj_start, count));
                };
            }
        }
        Ok(())
    }

    /// Loads an index object
    fn load_index_object(&mut self, index: &str, obj: &Map<String, Value>) -> Result<()> {
        for (k, v) in obj {
            let d = v.as_array().unwrap();
            let offset = d[0].as_i64().unwrap() as usize;
            let count = d[1].as_i64().unwrap() as usize;
            self.values
                .insert(k.clone(), (index.to_string(), offset, count));
        }
        Ok(())
    }

    /// Loads an index file
    fn load_index(&mut self, index: &str) -> Result<()> {
        let object = index.to_string() + INDEX_EXTENSION;
        let data = self
            .adapter
            .read()
            .unwrap()
            .read_object(object.as_str(), 0, 0)?;
        let json = std::str::from_utf8(&data)?;
        let json: Value = serde_json::from_str(json)?;
        if json.is_object() {
            self.load_index_object(index, json.as_object().unwrap())
        } else {
            bail!("index_not_an_object")
        }
    }

    /// Reloads the storage
    /// TODO: This can be partially replaced by a call to refresh
    pub fn reload(&mut self) -> Result<Vec<String>> {
        if !self.stage.is_empty() {
            bail!("non_empty_data_stage");
        }
        self.loaded_packs.clear();
        self.values.clear();
        let pack_list = self.adapter.read().unwrap().list_objects(PACK_EXTENSION)?;
        let index_list = self.adapter.read().unwrap().list_objects(INDEX_EXTENSION)?;
        let index_set = index_list.into_iter().collect::<HashSet<_>>();
        if !pack_list.is_empty() {
            for i in &pack_list {
                if index_set.contains(i) {
                    self.load_index(i)?;
                } else {
                    self.load_pack(i)?;
                }
                self.loaded_packs.insert(i.clone());
            }
        }
        Ok(pack_list)
    }

    pub fn get_loaded_packs(&self) -> &BTreeSet<String> {
        &self.loaded_packs
    }

    pub fn refresh(&mut self) -> Result<Vec<String>> {
        let pack_list = self.adapter.read().unwrap().list_objects(PACK_EXTENSION)?;
        let index_list = self.adapter.read().unwrap().list_objects(INDEX_EXTENSION)?;
        let index_set = index_list.into_iter().collect::<HashSet<_>>();
        let mut new_packs = vec![];
        if !pack_list.is_empty() {
            for i in &pack_list {
                if self.loaded_packs.contains(i) {
                    continue;
                }
                if index_set.contains(i) {
                    self.load_index(i)?;
                } else {
                    self.load_pack(i)?;
                }
                self.loaded_packs.insert(i.clone());
                new_packs.push(i.clone());
            }
        }
        Ok(new_packs)
    }

    pub fn unstage(&mut self) -> Result<()> {
        self.stage.clear();
        Ok(())
    }

    /// Returns true if the pack is readable and valid (digest matches)
    pub fn is_readable_and_valid_pack(&self, pack: &str) -> Result<bool> {
        let pack_name = pack.to_string() + PACK_EXTENSION;
        match self.adapter.read().unwrap().read_object(&pack_name, 0, 0) {
            Ok(data) => {
                let d = digest_bytes(data.as_slice());
                Ok(d.eq(pack))
            }
            Err(e) => Err(e),
        }
    }

    pub fn replicate(&mut self, other: &DataStorage) -> Result<()> {
        for p in &other.loaded_packs {
            if !self.loaded_packs.contains(p) {
                let rawdata = self.read_raw_bytes(p, 0, 0)?;
                self.write_raw_bytes(p, &rawdata)?;
            }
        }
        Ok(())
    }

    /// Writes an object associating it with the given revision (digest)
    pub fn write_object(
        &mut self,
        rev: &Revision,
        obj: Map<String, Value>
    ) -> Result<()> {
        if rev.is_resolved() || rev.is_deleted() || rev.is_empty() {
            Ok(())
        } else {
            // Otherwise store according to the object digest
            if rev.digest.len() <= 8 && u32::from_str_radix(&rev.digest, 16).is_ok() {
                Ok(())
            } else {
                self.write_raw_value(&rev.digest, obj.clone().into())?;
                {
                    let cache_l = self.cache.lock().unwrap();
                    let mut cache = cache_l.borrow_mut();
                    cache.put(rev.digest.to_string(), obj); // Only cache the full object
                }
                Ok(())
            }
        }
    }


/*
    fn build_reconstruction_path<'a>(
        &self,
        fromrev: &'a Revision,
        rt: &'a RevisionTree,
    ) -> Result<ReconstructionPath<'a>> {
        let mut reconstruction_path: Vec<&'a Revision> = vec![];
        assert!(!fromrev.is_resolved());
        let mut crev = fromrev;
        let cache_l = self.cache.lock().unwrap();
        let mut cache = cache_l.borrow_mut();
        loop {
            if crev.is_deleted() {
                // Special case, deleted object
                return Ok(ReconstructionPath {
                    origin: json!({"_deleted":true}).as_object().unwrap().clone(),
                    path: reconstruction_path,
                });
            } else if crev.is_empty() {
                // Special case, empty object
                return Ok(ReconstructionPath {
                    origin: Map::<String, Value>::new(),
                    path: reconstruction_path,
                });
            } else if cache.contains(&crev.digest) {
                return Ok(ReconstructionPath {
                    origin: cache.get(&crev.digest).unwrap().clone(),
                    path: reconstruction_path,
                });
            } else if !crev.index == 1 {
                // We reached the first revision (a non-delta revision)
                // Otherwise read the data from the backend adapter
                match self.read_data(&crev.digest)? {
                    Some(o) => {
                        let obj = o.as_object().ok_or_else(|| anyhow!("not_an_object"))?;
                        return Ok(ReconstructionPath {
                            origin: obj.clone(),
                            path: reconstruction_path,
                        });
                    }
                    None => {
                        return Err(anyhow!("failed_to_read_object {}", crev.to_string()));
                    }
                }
            } else {
                // Store this delta revision in the reconstruction path
                reconstruction_path.push(crev);
                // Retrieve the parent revision
                crev = match rt.get_parent(crev) {
                    Some(r) => r,
                    None => {
                        return Err(anyhow!(
                            "failed_to_determine_parent {} {:?}",
                            crev.to_string(),
                            reconstruction_path
                        ))
                    }
                };
            }
        }
    }


    /// Reads and returns an array descriptor, reconstructing deltas if necessary
    pub fn read_array_descriptor(&self, rev: &Revision, rt: &RevisionTree) -> Result<Vec<String>> {
        if rev.index == 1 { // First revision

        }


        // Determine the reconstruction path
        let rb = self.build_reconstruction_path(rev, rt)?;
        // Obtain the origin array
        let mut origin = rb.origin;
        let mut order_array;
        if let Some(base_array) = origin.get(ARRAY_DESCRIPTOR_ORDER_FIELD) {
            if let Some(base_array) = base_array.as_array() {
                order_array = base_array;
            } else {
                bail!(anyhow!("malformed_origin_array_descriptor"))
            }
        }
        if let Some(base_array) = origin.get(ARRAY_DESCRIPTOR_DELTA_ORDER_FIELD) {
            for r in rb.path.into_iter().rev() {
                // Obtain corresponding object
                if let Ok(Some(delta_descriptor)) = self.read_data(&r.digest) {
                    if let Some(delta_descriptor) = delta_descriptor.as_object() {
                        if let Some(patch) = delta_descriptor.get(ARRAY_DESCRIPTOR_ORDER_FIELD) {
                            if let Some(patch) = patch.as_array() {
                                // Apply patch
                                apply_diff_patch(&mut order_array, &patch)?;
                            } else {
                                bail!(anyhow!("malformed_array_descriptor"))
                            }
                        } else {
                            bail!(anyhow!("incomplete_array_descriptor"))
                        }
                    } else {
                        bail!(anyhow!("descriptor_not_an_object"))
                    }
                }
            }
        } else {
            bail!(anyhow!("invalid_origin_array_descriptor"))
        }
        // Update cache
        {
            let cache_l = self.cache.lock().unwrap();
            let mut cache = cache_l.borrow_mut();
            if !cache.contains(&rev.digest) {
                cache.put(rev.digest.to_string(), origin.clone());
            }
        }
        Ok(order_array.iter().map(|x| x.as_str().unwrap().to_string()).collect())
    }*/

    /// Reads an object at the given revision
    pub fn read_object(
        &self,
        revision: &Revision) -> Result<Map<String, Value>> {
        if revision.is_deleted() {
            // Special case, deleted object
            Ok(json!({"_deleted":true}).as_object().unwrap().clone())
        } else if revision.is_resolved() {
            // Special case, resolved object
            Ok(json!({"_resolved":true}).as_object().unwrap().clone())
        } else if revision.digest.len() <= 8 && u32::from_str_radix(&revision.digest, 16).is_ok() {
            // Special case, simple character
            let mut o = Map::<String, Value>::new();
            o.insert(HASH_FIELD.to_string(), Value::from(revision.digest.clone()));
            Ok(o)
        } else {
            let value = self.read_raw_value(&revision.digest)?;
            let object = value.as_object().expect("expecting_an_object");
            Ok(object.clone())
        }
    }

    /// Writes the given (JSON) value into the temporary pack (if not already there)
    pub fn write_raw_value(&mut self, digest: &str, obj: Value) -> Result<()> {
        if !self.values.contains_key(digest) && !self.stage.contains_key(digest) {
            self.stage.insert(digest.to_string(), obj);
        }
        Ok(())
    }

    /// Reads a JSON value given its digest
    pub fn read_raw_value(&self, digest: &str) -> Result<Value> {
        if self.values.contains_key(digest) {
            let (pack, offset, length) = self.values.get(digest).unwrap();
            let key = pack.clone() + PACK_EXTENSION;
            let data = self
                .adapter
                .read()
                .unwrap()
                .read_object(&key, *offset, *length)?;
            let json = std::str::from_utf8(&data)?;
            let json: Value = serde_json::from_str(json)?;
            Ok(json)
        } else if self.stage.contains_key(digest) {
            Ok(self.stage.get(digest).unwrap().clone())
        } else {
            Err(anyhow!("value_not_found"))
        }
    }

    /// Packs temporary data into a new pack with an index (committing to the adapter)
    /// Returns the identifier or the pack (digest of its contents)
    pub fn pack(&mut self) -> Result<Option<String>> {
        if self.stage.is_empty() {
            return Ok(None);
        }
        let mut index_map = Map::<String, Value>::new();
        let mut buf = Vec::<u8>::new();
        let mut start: usize = 1;
        buf.push(b'[');
        let mut remaining = self.stage.len();
        for (digest, v) in &self.stage {
            let content = serde_json::to_string(&v).unwrap();
            let bytes = content.as_bytes();
            buf.extend_from_slice(bytes);
            index_map.insert(digest.clone(), json!([start, bytes.len()]));
            remaining -= 1;
            if remaining > 0 {
                buf.push(b',');
                start = buf.len();
            }
        }
        buf.push(b']');
        let pack_digest = digest_bytes(buf.as_slice());
        let pack_key = pack_digest.clone() + PACK_EXTENSION;
        let adapter = self.adapter.write().unwrap();
        adapter.write_object(&pack_key, buf.as_slice())?;
        drop(adapter);
        if buf.len() > 800 * index_map.len() {
            // 80 bytes is the estimated size of an index entry, use index only if the size is 10 times bigger
            // Only write the index if worth it
            let index_key = pack_digest.clone() + INDEX_EXTENSION;
            let index_map_contents = serde_json::to_string(&index_map).unwrap();
            let adapter = self.adapter.write().unwrap();
            adapter.write_object(&index_key, index_map_contents.as_bytes())?;
            drop(adapter);
        }
        self.load_index_object(&pack_digest, &index_map)?;
        self.stage.clear();
        Ok(Some(pack_digest))
    }

    pub fn stage(&self) -> Result<Value> {
        let mut r = Map::<String, Value>::new();
        for (digest, v) in &self.stage {
            r.insert(digest.clone(), v.clone());
        }
        Ok(Value::from(r))
    }

    pub fn replay_stage(&mut self, s: &Value) -> Result<()> {
        if s.is_object() {
            let s = s.as_object().unwrap();
            for (digest, v) in s {
                if !self.values.contains_key(digest) {
                    self.stage.insert(digest.clone(), v.clone());
                }
            }
            Ok(())
        } else {
            Err(anyhow!("expecting_stage_object"))
        }
    }

    pub fn read_raw_bytes(&self, key: &str, offset: usize, length: usize) -> Result<Vec<u8>> {
        self.adapter
            .read()
            .unwrap()
            .read_object(key, offset, length)
    }

    pub fn write_raw_bytes(&mut self, key: &str, data: &[u8]) -> Result<()> {
        self.adapter.write().unwrap().write_object(key, data)
    }

    pub fn list_raw_items(&self, ext: &str) -> Result<Vec<String>> {
        self.adapter.read().unwrap().list_objects(ext)
    }
}
