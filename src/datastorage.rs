// Melda - Delta State JSON CRDT
// Copyright (C) 2021-2026 Amos Brocco <amos.brocco@supsi.ch>
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
use crate::constants::{HASH_FIELD, PACK_EXTENSION};
use crate::revision::Revision;
use crate::utils::digest_bytes;
use anyhow::{anyhow, bail, Result};
use lru::LruCache;
use serde_json::json;
use serde_json::Map;
use serde_json::Value;
use std::collections::BTreeSet;
use std::collections::HashMap;
use std::num::NonZeroUsize;
use std::sync::{Arc, Mutex, RwLock};

pub struct DataStorage {
    adapter: Arc<RwLock<Box<dyn Adapter>>>,
    stage: HashMap<String, Value>,
    committed_objects: HashMap<String, (String, usize, usize)>,
    applied_pack_ids: BTreeSet<String>,
    cache: Mutex<LruCache<String, Map<String, Value>>>,
}

impl DataStorage {
    /// Constructs a new Data storage based on the provided adapter
    pub fn new(adapter: Arc<RwLock<Box<dyn Adapter>>>) -> DataStorage {
        let cache_size = std::env::var("MELDA_DATA_CACHE_CAP")
            .unwrap_or_else(|_| "16".to_string())
            .parse::<u32>()
            .unwrap() as usize;
        DataStorage {
            adapter,
            stage: HashMap::<String, Value>::new(),
            committed_objects: HashMap::<String, (String, usize, usize)>::new(),
            applied_pack_ids: BTreeSet::new(),
            cache: Mutex::new(LruCache::<String, Map<String, Value>>::new(
                NonZeroUsize::new(cache_size).unwrap(),
            )),
        }
    }

    /// Tries to load a pack file, returns its contents
    pub fn try_load_pack(&self, pack: &str) -> Result<Vec<u8>> {
        let pack_name = pack.to_string() + PACK_EXTENSION;
        match self.adapter.read().unwrap().read_object(&pack_name, 0, 0) {
            Ok(data) => {
                let d = digest_bytes(data.as_slice());
                if d.eq(pack) {
                    Ok(data)
                } else {
                    Err(anyhow!("mismatching_digest"))
                }
            }
            Err(e) => Err(e),
        }
    }

    /// Parses and applies a pack
    fn parse_and_apply_pack(&mut self, name: &str, data: &[u8]) -> Result<()> {
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
                    self.committed_objects
                        .insert(digest, (name.to_string(), obj_start, count));
                };
            }
        }
        self.applied_pack_ids.insert(name.to_string());
        Ok(())
    }

    /// Returns the set of loaded packs
    pub fn applied_packs(&self) -> &BTreeSet<String> {
        &self.applied_pack_ids
    }

    /// Reloads the storage
    /// TODO: This can be partially replaced by a call to refresh
    pub fn reload(&mut self) -> Result<Vec<String>> {
        if !self.stage.is_empty() {
            bail!("non_empty_data_stage");
        }
        self.applied_pack_ids.clear();
        self.committed_objects.clear();
        let pack_list = self.adapter.read().unwrap().list_objects(PACK_EXTENSION)?;
        if !pack_list.is_empty() {
            for i in &pack_list {
                match self.try_load_pack(i) {
                    Ok(data) => {
                        if self.parse_and_apply_pack(i, &data).is_err() {
                            return Err(anyhow!("failed_to_apply_pack"));
                        }
                    }
                    Err(e) => return Err(e),
                }
            }
        }
        Ok(pack_list)
    }

    pub fn refresh(&mut self) -> Result<Vec<String>> {
        let pack_list = self.adapter.read().unwrap().list_objects(PACK_EXTENSION)?;
        let mut new_packs = vec![];
        if !pack_list.is_empty() {
            for i in &pack_list {
                if self.applied_pack_ids.contains(i) {
                    continue;
                }
                match self.try_load_pack(i) {
                    Ok(data) => {
                        if self.parse_and_apply_pack(i, &data).is_err() {
                            return Err(anyhow!("failed_to_apply_pack"));
                        }
                    }
                    Err(e) => return Err(e),
                }

                new_packs.push(i.clone());
            }
        }
        Ok(new_packs)
    }

    pub fn unstage(&mut self) -> Result<()> {
        self.stage.clear();
        Ok(())
    }

    /// Returns true if the revision is available and valid (digest matches)
    pub fn is_readable_and_valid_revision(&self, rev: &Revision) -> bool {
        if self.committed_objects.contains_key(rev.digest()) {
            true
        } else {
            matches!(self.read_object(rev), Ok(_obj))
        }
    }

    /// Writes an object associating it with the given revision (digest)
    pub fn write_object(&mut self, rev: &Revision, obj: Map<String, Value>) -> Result<()> {
        if rev.is_resolved() || rev.is_deleted() || rev.is_empty() || rev.is_charcode() {
            Ok(())
        } else {
            // Otherwise store according to the object digest
            self.write_raw_value(rev.digest(), obj.clone().into())?;
            {
                let mut cache = self.cache.lock().unwrap();
                cache.put(rev.digest().to_string(), obj); // Only cache the full object
            }
            Ok(())
        }
    }

    /// Reads an object at the given revision
    pub fn read_object(&self, revision: &Revision) -> Result<Map<String, Value>> {
        if revision.is_empty() {
            Ok(json!({}).as_object().unwrap().clone())
        } else if revision.is_deleted() {
            // Special case, deleted object
            Ok(json!({"_deleted":true}).as_object().unwrap().clone())
        } else if revision.is_resolved() {
            // Special case, resolved object
            Ok(json!({"_resolved":true}).as_object().unwrap().clone())
        } else if revision.is_charcode() {
            // Special case, simple character
            let mut o = Map::<String, Value>::new();
            o.insert(
                HASH_FIELD.to_string(),
                Value::from(revision.digest().clone()),
            );
            Ok(o)
        } else if let Some(object) = self.cache.lock().unwrap().get(revision.digest()) {
            Ok(object.clone())
        } else {
            let value = self.read_raw_value(revision.digest())?;
            let object = value.as_object().expect("expecting_an_object");
            Ok(object.clone())
        }
    }

    /// Writes the given (JSON) value into the temporary pack (if not already there)
    pub fn write_raw_value(&mut self, digest: &str, obj: Value) -> Result<()> {
        if !self.committed_objects.contains_key(digest) && !self.stage.contains_key(digest) {
            self.stage.insert(digest.to_string(), obj);
        }
        Ok(())
    }

    /// Reads a JSON value given its digest
    pub fn read_raw_value(&self, digest: &str) -> Result<Value> {
        if let Some(value) = self.committed_objects.get(digest) {
            let (pack, offset, length) = value;
            let key = format!("{}{}", pack, PACK_EXTENSION);
            let data = self
                .adapter
                .read()
                .unwrap()
                .read_object(&key, *offset, *length)?;
            // Verify content
            let computed = digest_bytes(&data);
            if computed != digest {
                return Err(anyhow!("corrupted_value"));
            }
            let json = std::str::from_utf8(&data)?;
            let json: Value = serde_json::from_str(json)?;
            Ok(json)
        } else if let Some(value) = self.stage.get(digest) {
            Ok(value.clone())
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
        let mut index_map = HashMap::<String, (usize, usize)>::new();
        let mut buf = Vec::<u8>::new();
        let mut start: usize = 1;
        buf.push(b'[');
        let mut remaining = self.stage.len();
        for (digest, v) in &self.stage {
            let content = serde_json::to_string(&v).unwrap();
            let bytes = content.as_bytes();
            buf.extend_from_slice(bytes);
            index_map.insert(digest.clone(), (start, bytes.len()));
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
        index_map.iter().for_each(|(key, (start, length))| {
            self.committed_objects.insert(
                key.clone(),
                (pack_digest.clone(), start.to_owned(), length.to_owned()),
            );
        });
        self.applied_pack_ids.insert(pack_digest.clone());
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

    pub fn has_staging(&self) -> bool {
        !self.stage.is_empty()
    }

    pub fn replay_stage(&mut self, s: &Value) -> Result<()> {
        if s.is_object() {
            let s = s.as_object().unwrap();
            for (digest, v) in s {
                if !self.committed_objects.contains_key(digest) {
                    self.stage.insert(digest.clone(), v.clone());
                }
            }
            Ok(())
        } else {
            Err(anyhow!("expecting_stage_object"))
        }
    }

    pub fn read_raw_item(&self, key: &str, offset: usize, length: usize) -> Result<Vec<u8>> {
        self.adapter
            .read()
            .unwrap()
            .read_object(key, offset, length)
    }

    pub fn write_raw_item(&mut self, key: &str, data: &[u8]) -> Result<()> {
        self.adapter.write().unwrap().write_object(key, data)
    }

    pub fn list_raw_items(&self, ext: &str) -> Result<Vec<String>> {
        self.adapter.read().unwrap().list_objects(ext)
    }

    /// Returns the underlying storage adapter
    pub fn get_adapter(&self) -> Arc<RwLock<Box<dyn Adapter>>> {
        self.adapter.clone()
    }
}
