// Melda - Delta State JSON CRDT
// Copyright (C) 2021 Amos Brocco <amos.brocco@supsi.ch>
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
use crate::revision::Revision;
use crate::revisiontree::RevisionTree;
use crate::utils::{
    apply_diff_patch, digest_bytes, is_flattened_field, make_diff_patch, merge_arrays,
};
use anyhow::{anyhow, bail, Result};
use lru::LruCache;
use serde_json::json;
use serde_json::Map;
use serde_json::Value;
use std::cell::RefCell;
use std::collections::{HashMap, HashSet};
use std::sync::{Arc, Mutex, RwLock};

const DELTA_PREFIX: &str = "\u{0394}";

pub struct DataStorage {
    adapter: Arc<RwLock<Box<dyn Adapter>>>,
    pack: HashMap<String, Value>,
    objects: HashMap<String, (String, usize, usize)>,
    cache: Mutex<RefCell<LruCache<String, Map<String, Value>>>>,
    force_full_array_interval: u32,
}

enum FetchedObject {
    Delta(Map<String, Value>),
    Full(Map<String, Value>),
}

struct ReconstructionPath<'a> {
    origin: Map<String, Value>,
    path: Vec<&'a Revision>,
}

impl DataStorage {
    /// Constructs a new Data storage based on the provided adapter
    pub fn new(adapter: Arc<RwLock<Box<dyn Adapter>>>) -> DataStorage {
        let cache_size = std::env::var("MELDA_DATA_CACHE_CAP")
            .unwrap_or("16".to_string())
            .parse::<u32>()
            .unwrap() as usize;
        let full_array_interval = std::env::var("MELDA_FORCE_FULL_ARRAY_INTERVAL")
            .unwrap_or("1000".to_string())
            .parse::<u32>()
            .unwrap();
        DataStorage {
            adapter,
            pack: HashMap::<String, Value>::new(),
            objects: HashMap::<String, (String, usize, usize)>::new(),
            cache: Mutex::new(RefCell::new(LruCache::<String, Map<String, Value>>::new(
                cache_size,
            ))),
            force_full_array_interval: full_array_interval,
        }
    }

    /// Merges another DataStorage into this one
    pub fn merge(&mut self, other: &DataStorage) -> Result<()> {
        for (digest, _) in &other.objects {
            if !self.objects.contains_key(digest) && !self.pack.contains_key(digest) {
                self.write_data(digest, other.read_data(digest)?.unwrap())?;
            }
        }
        for (digest, _) in &other.pack {
            if !self.objects.contains_key(digest) && !self.pack.contains_key(digest) {
                self.write_data(digest, other.read_data(digest)?.unwrap())?;
            }
        }
        Ok(())
    }

    /// Loads a pack file (and rebuilds the index)
    fn load_pack(&mut self, pack: &String) -> Result<()> {
        let object = pack.clone() + ".pack";
        let data = self
            .adapter
            .read()
            .unwrap()
            .read_object(object.as_str(), 0, 0)?;
        self.load_pack_data(pack, &data)
    }

    /// Data is the raw string (we need to compute the offset and length of the object)
    fn load_pack_data(&mut self, name: &String, data: &Vec<u8>) -> Result<()> {
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
                    self.objects
                        .insert(digest, (name.clone(), obj_start, count));
                };
            }
        }
        Ok(())
    }

    /// Loads an index object
    fn load_index_object(&mut self, index: &String, obj: &Map<String, Value>) -> Result<()> {
        for (k, v) in obj {
            let d = v.as_array().unwrap();
            let offset = d[0].as_i64().unwrap() as usize;
            let count = d[1].as_i64().unwrap() as usize;
            self.objects
                .insert(k.clone(), (index.clone(), offset, count));
        }
        Ok(())
    }

    /// Loads an index file
    fn load_index(&mut self, index: &String) -> Result<()> {
        let object = index.clone() + ".index";
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
    pub fn reload(&mut self) -> Result<()> {
        self.pack.clear();
        self.objects.clear();
        let pack_list = self.adapter.read().unwrap().list_objects(".pack")?;
        let index_list = self.adapter.read().unwrap().list_objects(".index")?;
        let index_set = index_list.into_iter().collect::<HashSet<_>>();
        if !pack_list.is_empty() {
            for i in &pack_list {
                if index_set.contains(i) {
                    self.load_index(i)?;
                } else {
                    self.load_pack(i)?;
                }
            }
        }
        Ok(())
    }

    /// Returns true if the pack is readable and valid (digest matches)
    pub fn is_readable_and_valid_pack(&self, pack: &str) -> Result<bool> {
        let pack_name = pack.to_string() + ".pack";
        match self.adapter.read().unwrap().read_object(&pack_name, 0, 0) {
            Ok(data) => {
                let d = digest_bytes(data.as_slice());
                Ok(d.eq(pack))
            }
            Err(e) => Err(e),
        }
    }

    /// Writes an object associating it with the given revision (digest)
    pub fn write_object(
        &mut self,
        rev: &Revision,
        obj: Map<String, Value>,
        delta: Option<Map<String, Value>>,
    ) -> Result<()> {
        if rev.is_resolved() || rev.is_deleted() || rev.is_empty() {
            Ok(())
        } else {
            if rev.is_delta() && delta.is_some() {
                // If the revision is a delta revision, store according to the delta digest
                let delta_obj = delta.unwrap();
                self.write_data(
                    rev.delta_digest.as_ref().unwrap(),
                    Value::from(delta_obj.clone()),
                )?;
                {
                    let cache_l = self.cache.lock().unwrap();
                    let mut cache = cache_l.borrow_mut();
                    cache.put(rev.digest.to_string(), obj); // Only cache the full object
                }
                Ok(())
            } else {
                // Otherwise store according to the full object digest
                if rev.digest.len() <= 8 && u32::from_str_radix(&rev.digest, 16).is_ok() {
                    Ok(())
                } else {
                    self.write_data(&rev.digest, obj.clone().into())?;
                    {
                        let cache_l = self.cache.lock().unwrap();
                        let mut cache = cache_l.borrow_mut();
                        cache.put(rev.digest.to_string(), obj); // Only cache the full object
                    }
                    Ok(())
                }
            }
        }
    }

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
            } else if !crev.is_delta() {
                // We reached the first revision or a non-delta revision
                // Otherwise read the data from the backend adapter
                match self.read_data(&crev.digest)? {
                    Some(o) => {
                        let obj = o.as_object().ok_or(anyhow!("not_an_object"))?;
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
                crev = match rt.parent(crev) {
                    Some(r) => r,
                    None => return Err(anyhow!("failed_to_determine_parent {}", crev.to_string())),
                };
            }
        }
    }

    /// Returns the object at the given revision (the resulting object is not undelta nor merged)
    fn read_object_or_delta(&self, rev: &Revision) -> Result<FetchedObject> {
        assert!(!rev.is_resolved());
        if rev.is_deleted() {
            // Special case, deleted object
            Ok(FetchedObject::Full(
                json!({"_deleted":true}).as_object().unwrap().clone(),
            ))
        } else if rev.is_empty() {
            // Special case, empty object
            Ok(FetchedObject::Full(Map::<String, Value>::new()))
        } else {
            // Try to read the object by full digest
            match self.read_data(&rev.digest)? {
                Some(o) => {
                    let obj = o.as_object().ok_or(anyhow!("not_an_object"))?;
                    Ok(FetchedObject::Full(obj.clone()))
                }
                None => {
                    // Try to read the object by delta digest
                    match &rev.delta_digest {
                        Some(dd) => match self.read_data(&dd)? {
                            Some(o) => {
                                let obj = o.as_object().ok_or(anyhow!("not_an_object"))?;
                                Ok(FetchedObject::Delta(obj.clone()))
                            }
                            None => Err(anyhow!("failed_to_read_delta_object {}", dd)),
                        },
                        None => Err(anyhow!("failed_to_read_object {}", rev.to_string())),
                    }
                }
            }
        }
    }

    /// Reads and returns an object, resolving deltas if necessary
    fn read_full_object(&self, rev: &Revision, rt: &RevisionTree) -> Result<Map<String, Value>> {
        let rb = self.build_reconstruction_path(rev, rt)?;
        let mut origin = rb.origin;
        for r in rb.path.into_iter().rev() {
            origin = self.apply_delta(&r, &origin)?
        }
        {
            let cache_l = self.cache.lock().unwrap();
            let mut cache = cache_l.borrow_mut();
            if !cache.contains(&rev.digest) {
                cache.put(rev.digest.to_string(), origin.clone());
            }
        }
        Ok(origin)
    }

    // Applies a delta revision object to a reference object
    fn apply_delta(
        &self,
        delta_revision: &Revision,
        delta_reference_object: &Map<String, Value>,
    ) -> Result<Map<String, Value>> {
        let obj = self.read_object_or_delta(delta_revision)?;
        match obj {
            FetchedObject::Delta(obj) => {
                obj.into_iter()
                    .map(|(k, v)| {
                        if k.starts_with(DELTA_PREFIX) {
                            let changes = v.as_array().ok_or(anyhow!("not_an_array"))?;
                            let non_delta_corresponding_field =
                                k.strip_prefix(DELTA_PREFIX).unwrap();
                            // Get the reference field (either as delta or non delta)
                            let base_array = if delta_reference_object.contains_key(&k) {
                                delta_reference_object
                                    .get(&k)
                                    .unwrap()
                                    .as_array()
                                    .ok_or(anyhow!("not_an_array"))?
                            } else if delta_reference_object
                                .contains_key(non_delta_corresponding_field)
                            {
                                delta_reference_object
                                    .get(non_delta_corresponding_field)
                                    .unwrap()
                                    .as_array()
                                    .ok_or(anyhow!("not_an_array"))?
                            } else {
                                bail!("missing_referenced_field")
                            };
                            // Apply patch
                            let mut base_array = base_array.clone();
                            apply_diff_patch(&mut base_array, &changes)?;
                            Ok((k, Value::from(base_array).clone()))
                        } else {
                            Ok((k, v))
                        }
                    })
                    .collect()
            }
            FetchedObject::Full(_) => bail!("expecting_delta_object"),
        }
    }

    /// Merges the arrays within leaf revisions
    /// This is needed because we do not want that objects added to the array
    /// just disappear when there are conflicting revisions
    fn read_merged_object(
        &self,
        base_revision: &Revision,
        rt: &RevisionTree,
    ) -> Result<Map<String, Value>> {
        let leafs = rt.leafs();
        // The base object corresponds to the revision we want to keep
        let base_object = self.read_full_object(base_revision, rt)?;
        if leafs.len() > 1 {
            // If there are multiple leafs, merge
            let merged_object = base_object
                .into_iter()
                .map(|(k, v)| -> Result<(String, Value)> {
                    // Iterate over all fields and if the field is a flatten field, try to merge the contents
                    if is_flattened_field(k.as_str()) {
                        let mut base_array = v.as_array().unwrap().clone();
                        for leaf_revision in &leafs {
                            if **leaf_revision != *base_revision {
                                let leaf_object = self.read_full_object(leaf_revision, rt)?;
                                // Look for the corresponding field in the leaf object (only matching fields will be merged)
                                match leaf_object.get(&k) {
                                    Some(v) => {
                                        if v.is_array() {
                                            // Match and is an array, merge
                                            merge_arrays(v.as_array().unwrap(), &mut base_array)
                                        }
                                    }
                                    None => {
                                        // Non matching field
                                        // If we have a delta field in the base_object then maybe the leaf has a non-delta field
                                        if k.starts_with(DELTA_PREFIX) {
                                            let non_delta_corresponding_field = k
                                                .strip_prefix(DELTA_PREFIX)
                                                .ok_or(anyhow!("prefix_disappeared"))?;
                                            match leaf_object.get(non_delta_corresponding_field) {
                                                Some(v) => {
                                                    if v.is_array() {
                                                        merge_arrays(
                                                            v.as_array().unwrap(),
                                                            &mut base_array,
                                                        )
                                                    }
                                                }
                                                None => {}
                                            }
                                        } else {
                                            // If we have a non-delta field in the base_object then maybe the leaf has a delta field
                                            let delta_corresponding_field =
                                                DELTA_PREFIX.to_string() + k.as_str();
                                            match leaf_object.get(&delta_corresponding_field) {
                                                Some(v) => {
                                                    if v.is_array() {
                                                        merge_arrays(
                                                            v.as_array().unwrap(),
                                                            &mut base_array,
                                                        )
                                                    }
                                                }
                                                None => {}
                                            }
                                        }
                                    }
                                }
                            }
                        }
                        // Done merging
                        Ok((k, Value::from(base_array)))
                    } else {
                        Ok((k, v))
                    }
                });
            let result: Map<String, Value> =
                merged_object.collect::<Result<Map<String, Value>>>()?;
            Ok(result)
        } else {
            Ok(base_object)
        }
    }

    /// Reads an object at the given revision
    pub fn read_object(
        &self,
        revision: &Revision,
        rt: &RevisionTree,
    ) -> Result<Map<String, Value>> {
        if revision.digest.len() <= 8 && u32::from_str_radix(&revision.digest, 16).is_ok() {
            let mut o = Map::<String, Value>::new();
            o.insert("#".to_string(), Value::from(revision.digest.clone()));
            Ok(o)
        } else {
            self.read_merged_object(revision, rt)
        }
    }

    /// Constructs a delta object by replacing delta field values with patches from the current winner
    /// The winner is determined from the revision tree
    /// If a delta field cannot be replaced by a delta (because the current winner has no such field)
    /// the field is replaced with a non delta corresponding field
    /// The reference object (winner) is lazily costructed as soon as a delta field is detected
    pub fn delta_object(
        &self,
        obj: Map<String, Value>,
        rt: &RevisionTree,
    ) -> Result<Option<Map<String, Value>>> {
        // Reference object that might be loaded and used if there is a delta field
        let mut delta_reference_object: Option<Map<String, Value>> = None;
        let delta_reference_revision = rt.winner();
        if let Some(r) = delta_reference_revision {
            if self.force_full_array_interval != 0 && r.index % self.force_full_array_interval == 0
            {
                // Force a full object every N revisions
                return Ok(None);
            }
        }
        let mut contains_delta = false;
        // Process all fields of the object
        let delta = obj
            .into_iter()
            .map(|(k, v)| {
                if k.starts_with(DELTA_PREFIX) {
                    // If the field key starts with a delta prefix
                    let non_delta_corresponding_field = k.strip_prefix(DELTA_PREFIX).unwrap();
                    let array = v.as_array().ok_or(anyhow!("not_an_array"))?;
                    if delta_reference_revision.is_none() {
                        // Special case for first revision
                        // Return an non delta field with all the contents of the array
                        return Ok((k, Value::from(array.clone())));
                    }
                    // When not in the special case, construct the reference object upon which
                    // changes will be constructed
                    if delta_reference_object.is_none() {
                        // Lazy construction based on the reference revision (the current winner)
                        delta_reference_object =
                            Some(self.read_full_object(delta_reference_revision.unwrap(), rt)?)
                    }
                    let delta_reference_object = delta_reference_object.as_ref().unwrap();
                    // Try the same key first, then the non-delta key
                    for key in [&k, non_delta_corresponding_field] {
                        if delta_reference_object.contains_key(key) {
                            // Obtain value of the field
                            let delta_reference_array = delta_reference_object
                                .get(key)
                                .unwrap()
                                .as_array()
                                .ok_or(anyhow!("not_an_array"))?;
                            // Compute changes
                            let changes = make_diff_patch(delta_reference_array, &array)?;
                            contains_delta = true;
                            // If we store changes, always assign them to the delta key
                            return Ok((k.to_string(), Value::from(changes)));
                        }
                    }
                    // If we are here the delta reference object did not contain a compatible field
                    // We are forced to skip the delta
                    return Ok((k, Value::from(array.clone())));
                } else {
                    // Non delta field
                    Ok((k, v))
                }
            })
            .collect::<Result<Map<String, Value>>>()?;
        if contains_delta {
            Ok(Some(delta))
        } else {
            Ok(None)
        }
    }

    /// Writes the given value (object) into the temporary pack (if not already there)
    pub fn write_data(&mut self, digest: &str, obj: Value) -> Result<()> {
        if !self.objects.contains_key(digest) && !self.pack.contains_key(digest) {
            self.pack.insert(digest.to_string(), obj);
        }
        Ok(())
    }

    /// Reads a value given its digest
    pub fn read_data(&self, digest: &str) -> Result<Option<Value>> {
        if self.objects.contains_key(digest) {
            let (pack, offset, length) = self.objects.get(digest).unwrap();
            let key = pack.clone() + ".pack";
            let data = self
                .adapter
                .read()
                .unwrap()
                .read_object(&key, *offset, *length)?;
            let json = std::str::from_utf8(&data)?;
            let json: Value = serde_json::from_str(json)?;
            if json.is_object() {
                Ok(Some(json))
            } else {
                bail!("not_an_object")
            }
        } else if self.pack.contains_key(digest) {
            Ok(Some(self.pack.get(digest).unwrap().clone()))
        } else {
            Ok(None)
        }
    }

    /// Packs temporary data into a new pack with an index (committing to the adapter)
    /// Returns the identifier or the pack (digest of its contents)
    pub fn pack(&mut self) -> Result<Option<String>> {
        if self.pack.is_empty() {
            return Ok(None);
        }
        let mut index_map = Map::<String, Value>::new();
        let mut buf = Vec::<u8>::new();
        let mut start: usize = 1;
        buf.push(b'[');
        let mut remaining = self.pack.len();
        for (digest, v) in &self.pack {
            let content = serde_json::to_string(&v).unwrap();
            let bytes = content.as_bytes();
            buf.extend_from_slice(&bytes);
            index_map.insert(digest.clone(), json!([start, bytes.len()]));
            remaining -= 1;
            if remaining > 0 {
                buf.push(b',');
                start = buf.len();
            }
        }
        buf.push(b']');
        let pack_digest = digest_bytes(buf.as_slice());
        let pack_key = pack_digest.clone() + ".pack";
        let adapter = self.adapter.write().unwrap();
        adapter.write_object(&pack_key, buf.as_slice())?;
        drop(adapter);
        if buf.len() > 800 * index_map.len() {
            // 80 bytes is the estimated size of an index entry, use index only if the size is 10 times bigger
            // Only write the index if worth it
            let index_key = pack_digest.clone() + ".index";
            let index_map_contents = serde_json::to_string(&index_map).unwrap();
            let adapter = self.adapter.write().unwrap();
            adapter.write_object(&index_key, index_map_contents.as_bytes())?;
            drop(adapter);
        }
        self.load_index_object(&pack_digest, &index_map)?;
        self.pack.clear();
        Ok(Some(pack_digest))
    }

    pub fn stage(&self) -> Result<Value> {
        let mut r = Map::<String, Value>::new();
        for (digest, v) in &self.pack {
            r.insert(digest.clone(), v.clone());
        }
        Ok(Value::from(r))
    }

    pub fn replay_stage(&mut self, s: &Value) -> Result<()> {
        if s.is_object() {
            let s = s.as_object().unwrap();
            for (digest, v) in s {
                self.pack.insert(digest.clone(), v.clone());
            }
            Ok(())
        } else {
            Err(anyhow!("expecting_stage_object"))
        }
    }

    pub fn read_raw_object(&self, key: &str, offset: usize, length: usize) -> Result<Vec<u8>> {
        self.adapter
            .read()
            .unwrap()
            .read_object(key, offset, length)
    }

    pub fn write_raw_object(&mut self, key: &str, data: &[u8]) -> Result<()> {
        self.adapter.write().unwrap().write_object(key, data)
    }

    pub fn list_raw_objects(&self, ext: &str) -> Result<Vec<String>> {
        self.adapter.read().unwrap().list_objects(ext)
    }
}
