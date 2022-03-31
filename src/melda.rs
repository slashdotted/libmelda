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
use crate::datastorage::DataStorage;
use crate::revision::Revision;
use crate::revisiontree::RevisionTree;
use crate::utils::{digest_bytes, digest_object, digest_string, flatten, unflatten};
use crate::constants::{ID_FIELD, ROOT_ID, ROOT_FIELD, CHANGESETS_FIELD, DELTA_EXTENSION, INFORMATION_FIELD, FULL_CHANGESETS_FIELD, PACK_FIELD, OBJECTS_FIELD};
use anyhow::{anyhow, bail, Result};
use rayon::prelude::*;
use serde_json::{Map, Value};
use std::collections::{HashMap, HashSet};
use std::sync::{Arc, Mutex, RwLock};

pub struct Melda {
    documents: RwLock<HashMap<String, RevisionTree>>,
    data: RwLock<DataStorage>,
    root_identifier: RwLock<String>,
    revision_update_records: RwLock<Vec<(String, Revision, Option<Revision>)>>,
    loaded_blocks: Vec<String>,
}

// To implement snapshots (for example, to revert to a previous version)
//
// 1. After commit get the list of blocks using blocks()
// 2. Save the list inside a special undo object
// for example:
// {
//	"author" : "Amos",
//	"date" : "2022-03-30-12:51",
//	"description" : "Description of the update",
//	"blocks" : [ ... list of blocks ... ]
// }
// 3. Store the object using the adapter methods (for exmaple with key HASH.undo, where hash
//    is the digest of the object)
// 4. If needed, retrieve undo points by listing objects by .undo extension (from the adapter).
//    Objects can be sorted using one of the fields in the undo object.      
// 5. (Optional) Verify that the required blocks are available (using adapter.list_objects(...))
// 6. Reload only the selected blocks using melda.reload_only(...)

impl Melda {
    /// Initializes a new data structure
    /// The adapter is used to initialize the Data Storage
    pub fn new(adapter: Arc<RwLock<Box<dyn Adapter>>>) -> Result<Melda> {
        let mut dc = Melda {
            documents: RwLock::new(HashMap::<String, RevisionTree>::new()),
            data: RwLock::new(DataStorage::new(adapter.clone())),
            root_identifier: RwLock::new(ROOT_ID.to_string()),
            revision_update_records: RwLock::new(Vec::<(String, Revision, Option<Revision>)>::new()),
            loaded_blocks: Vec::new()
        };
        dc.reload()?;
        Ok(dc)
    }

    /// Records the creation of an object
    pub fn create_object(&mut self, uuid: String, obj: Map<String, Value>) -> Result<()> {
        let mut rt = RevisionTree::new();
        let rev = Revision::new(1u32, digest_object(&obj)?, None);
        let mut data = self.data.write().unwrap();
        data.write_object(&rev, obj, None)?;
        rt.add(rev.clone(), None);
        if self
            .documents
            .write()
            .unwrap()
            .insert(uuid.clone(), rt)
            .is_some()
        {
            bail!("duplicate_revision_tree");
        }
        self.revision_update_records
            .write()
            .unwrap()
            .push((uuid, rev, None));
        Ok(())
    }

    /// Records the update of an object
    pub fn update_object(&mut self, uuid: String, obj: Map<String, Value>) -> Result<()> {
        let docs = self.documents.read().unwrap();
        match docs.get(&uuid) {
            Some(rt) => {
                let digest = digest_object(&obj)?; // Digest of the "full" object
                let w = rt.winner().unwrap().clone(); // Winning revision
                if digest.ne(&w.digest) {
                    // The w.digest corresponds to the "full" object
                    let data = self.data.read().unwrap();
                    match data.delta_object(obj.clone(), rt)? {
                        Some(delta) => {
                            // The delta was created
                            let delta_digest = digest_object(&delta)?;
                            let rev = Revision::new_with_delta(
                                w.index + 1,
                                digest,
                                delta_digest,
                                Some(&w),
                            );
                            drop(docs);
                            self.documents
                                .write()
                                .unwrap()
                                .get_mut(&uuid)
                                .unwrap()
                                .add(rev.clone(), Some(w.clone()));
                            drop(data);
                            let mut data = self.data.write().unwrap();
                            data.write_object(&rev, obj, Some(delta))?;
                            self.revision_update_records.write().unwrap().push((
                                uuid,
                                rev,
                                Some(w),
                            ));
                        }
                        None => {
                            // There were no delta fields or the object should not be "delta-ed"
                            let rev = Revision::new(w.index + 1, digest, Some(&w));
                            let mut docs = self.documents.write().unwrap();
                            let rt = docs.get_mut(&uuid).unwrap();
                            rt.add(rev.clone(), Some(w.clone()));
                            drop(data);
                            let mut data = self.data.write().unwrap();
                            data.write_object(&rev, obj, None)?;
                            self.revision_update_records.write().unwrap().push((
                                uuid,
                                rev,
                                Some(w),
                            ));
                        }
                    }
                }
                Ok(())
            }
            None => {
                drop(docs);
                self.create_object(uuid, obj)
            }
        }
    }

    /// Records the removal of an object
    pub fn delete_object(&mut self, uuid: &String) -> Result<()> {
        match self.documents.write().unwrap().get_mut(uuid) {
            Some(rt) => {
                let w = rt.winner().unwrap().clone();
                if !w.is_deleted() && !w.is_resolved() {
                    let rev = Revision::new_deleted(&w);
                    rt.add(rev.clone(), Some(w.clone()));
                    self.revision_update_records.write().unwrap().push((
                        uuid.clone(),
                        rev,
                        Some(w),
                    ));
                }
                Ok(())
            }
            None => Err(anyhow!("object_not_found")),
        }
    }

    /// Commits data to the adapter
    pub fn commit(
        &mut self,
        information: Option<Map<String, Value>>,
        full_record: bool,
    ) -> Result<Option<String>> {
        let revision_update_records = self.revision_update_records.read().unwrap();
        if revision_update_records.is_empty() {
            return Ok(None);
        }
        drop(revision_update_records);
        let mut block = Map::<String, Value>::new();
        let mut data = self.data.write().unwrap();
        let _packid = data.pack()?;
        if full_record {
            // Full record
            let mut changes = Vec::<Value>::new();
            for (uuid, rev, _) in self.revision_update_records.read().unwrap().iter() {
                let mut path: Vec<String> = self
                    .documents
                    .write()
                    .unwrap()
                    .get(uuid)
                    .unwrap()
                    .get_path(&rev)
                    .iter()
                    .map(|x| x.to_string())
                    .collect();
                path.insert(0, uuid.clone());
                changes.push(Value::from(path));
            }
            block.insert(FULL_CHANGESETS_FIELD.to_string(), Value::from(changes));
        } else {
            // Partial (delta) record
            let mut changes = Vec::<Value>::new();
            for (uuid, rev, prev) in self.revision_update_records.read().unwrap().iter() {
                if prev.is_none() {
                    // Creation record
                    let tuple = vec![uuid.clone(), rev.digest.clone()];
                    changes.push(Value::from(tuple));
                } else {
                    // Update record
                    if rev.is_delta() {
                        let quad = vec![
                            uuid.clone(),
                            prev.as_ref().unwrap().to_string(),
                            rev.digest.clone(),
                            rev.delta_digest.as_ref().unwrap().clone(),
                        ];
                        changes.push(Value::from(quad));
                    } else {
                        let triple = vec![
                            uuid.clone(),
                            prev.as_ref().unwrap().to_string(),
                            rev.digest.clone(),
                        ];
                        changes.push(Value::from(triple));
                    }
                }
            }
            block.insert(CHANGESETS_FIELD.to_string(), Value::from(changes));
        }
        if information.is_some() {
            block.insert(INFORMATION_FIELD.to_string(), Value::from(information.unwrap()));
        }
        if self.root_identifier.read().unwrap().ne(ROOT_ID) {
            block.insert(
                ROOT_FIELD.to_string(),
                Value::from(self.root_identifier.read().unwrap().clone()),
            );
        }
        let blockstr = serde_json::to_string(&block).unwrap();
        let blockid = digest_string(&blockstr) + DELTA_EXTENSION;
        data.write_raw_object(&blockid, blockstr.as_bytes())?;
        self.revision_update_records.write().unwrap().clear();
        log::debug!("commit {}", blockid);
        self.loaded_blocks.push(blockid.clone());
        Ok(Some(blockid))
    }

    /// Loads a block
    fn load_block(&mut self, blockid: &String) -> Result<()> {
        let object = blockid.clone() + DELTA_EXTENSION;
        let data = self.data.read().unwrap();
        let data = data.read_raw_object(object.as_str(), 0, 0)?;
        let json = std::str::from_utf8(&data)?;
        let json: Value = serde_json::from_str(json)?;
        if json.is_object() {
            let blockobj = json.as_object().unwrap();
            let digest = digest_bytes(data.as_slice());
            if digest.eq(blockid) {
                if blockobj.contains_key(FULL_CHANGESETS_FIELD) || blockobj.contains_key(CHANGESETS_FIELD) {
                    if blockobj.contains_key(PACK_FIELD) {
                        let packs = blockobj.get(PACK_FIELD).ok_or(anyhow!("missing_pack_reference"))?;
                        if packs.is_array() {
                            let packs = packs.as_array().ok_or(anyhow!("packs_not_an_array"))?;
                            if !packs.iter().all(|x| {
                                if x.is_string() {
                                    let data = self.data.read().unwrap();
                                    match data.is_readable_and_valid_pack(x.as_str().unwrap()) {
                                        Ok(v) => v,
                                        Err(_) => false,
                                    }
                                } else {
                                    false
                                }
                            }) {
                                bail!("missing_packs");
                            }
                        }
                    }
                    if blockobj.contains_key(ROOT_FIELD) {
                        let rootid = blockobj.get(ROOT_FIELD).ok_or(anyhow!("missing_root_id"))?;
                        if !rootid.is_string() {
                            bail!("root_is_not_string");
                        }
                        let mut rid = self.root_identifier.write().unwrap();
                        *rid = rootid.as_str().unwrap().to_string();
                    }
                    let changes = blockobj.get(FULL_CHANGESETS_FIELD);
                    if changes.is_some() && changes.unwrap().is_array() {
                        for c in changes.unwrap().as_array().unwrap() {
                            if c.is_array() {
                                let record = c.as_array().unwrap();
                                if record.len() >= 2 {
                                    let uuid = &record[0];
                                    let path = &record[1..];
                                    if uuid.is_string() {
                                        let uuid = uuid.as_str().unwrap();
                                        if !self.documents.read().unwrap().contains_key(uuid) {
                                            let mut rt = RevisionTree::new();
                                            rt.load_path(
                                                path.iter()
                                                    .map(|x| x.as_str().unwrap().to_string())
                                                    .collect(),
                                            );
                                            self.documents
                                                .write()
                                                .unwrap()
                                                .insert(uuid.to_string(), rt);
                                        } else {
                                            let mut docs = self.documents.write().unwrap();
                                            let rt = docs.get_mut(uuid).unwrap();
                                            rt.load_path(
                                                path.iter()
                                                    .map(|x| x.as_str().unwrap().to_string())
                                                    .collect(),
                                            );
                                        }
                                    }
                                }
                            }
                        }
                    }
                    let changes = blockobj.get(CHANGESETS_FIELD);
                    if changes.is_some() && changes.unwrap().is_array() {
                        for c in changes.unwrap().as_array().unwrap() {
                            if c.is_array() {
                                let record = c.as_array().unwrap();
                                if record.len() == 2 {
                                    let uuid = record[0]
                                        .as_str()
                                        .ok_or(anyhow!("expecting_uuid_string"))?;
                                    let digest = record[1]
                                        .as_str()
                                        .ok_or(anyhow!("expecting_digest_string"))?;
                                    let r = Revision::new(1, digest.to_string(), None);
                                    if !self.documents.read().unwrap().contains_key(uuid) {
                                        let mut rt = RevisionTree::new();
                                        rt.add(r, None);
                                        self.documents
                                            .write()
                                            .unwrap()
                                            .insert(uuid.to_string(), rt);
                                    } else {
                                        let mut docs = self.documents.write().unwrap();
                                        let rt = docs.get_mut(uuid).unwrap();
                                        rt.add(r, None);
                                    }
                                } else if record.len() == 3 {
                                    let uuid = record[0]
                                        .as_str()
                                        .ok_or(anyhow!("expecting_uuid_string"))?;
                                    let prev = record[1]
                                        .as_str()
                                        .ok_or(anyhow!("expecting_revision_string"))?;
                                    let digest = record[2]
                                        .as_str()
                                        .ok_or(anyhow!("expecting_digest_string"))?;
                                    let prev = Revision::from(prev)?;
                                    let r = Revision::new(
                                        prev.index + 1,
                                        digest.to_string(),
                                        Some(&prev),
                                    );
                                    if !self.documents.read().unwrap().contains_key(uuid) {
                                        let mut rt = RevisionTree::new();
                                        rt.add(r, Some(prev));
                                        self.documents
                                            .write()
                                            .unwrap()
                                            .insert(uuid.to_string(), rt);
                                    } else {
                                        let mut docs = self.documents.write().unwrap();
                                        let rt = docs.get_mut(uuid).unwrap();
                                        rt.add(r, Some(prev));
                                    }
                                } else if record.len() == 4 {
                                    let uuid = record[0]
                                        .as_str()
                                        .ok_or(anyhow!("expecting_uuid_string"))?;
                                    let prev = record[1]
                                        .as_str()
                                        .ok_or(anyhow!("expecting_revision_string"))?;
                                    let digest = record[2]
                                        .as_str()
                                        .ok_or(anyhow!("expecting_digest_string"))?;
                                    let delta_digest = record[3]
                                        .as_str()
                                        .ok_or(anyhow!("expecting_digest_string"))?;
                                    let prev = Revision::from(prev)?;
                                    let r = Revision::new_with_delta(
                                        prev.index + 1,
                                        digest.to_string(),
                                        delta_digest.to_string(),
                                        Some(&prev),
                                    );
                                    if !self.documents.read().unwrap().contains_key(uuid) {
                                        let mut rt = RevisionTree::new();
                                        rt.add(r, Some(prev));
                                        self.documents
                                            .write()
                                            .unwrap()
                                            .insert(uuid.to_string(), rt);
                                    } else {
                                        let mut docs = self.documents.write().unwrap();
                                        let rt = docs.get_mut(uuid).unwrap();
                                        rt.add(r, Some(prev));
                                    }
                                } else {
                                    bail!("invalid_changes_record")
                                }
                            }
                        }
                    }
                }
            }
        } else {
            bail!("json is not object {}", json);
        }
        Ok(())
    }

    pub fn all_docs(&self) -> Vec<String> {
        self.documents
            .read()
            .unwrap()
            .iter()
            .map(|(k, _)| k.clone())
            .collect()
    }

    pub fn value(&self, uuid: &str, revision: &str) -> Result<Map<String, Value>> {
        let revision = Revision::from(revision).expect("invalid_revision_string");
        match self.documents.read().unwrap().get(uuid) {
            Some(o) => {
                self.data.read().unwrap().read_object(&revision, o)
            }
            None => Err(anyhow!("invalid object uuid")),
        }
    }

    /// Reloads the data structure
    pub fn reload(&mut self) -> Result<()> {
        self.reload_only(None)
    }

    pub fn blocks(&self) -> Vec<String> {
        self.loaded_blocks.clone()
    }

    pub fn reload_only(&mut self, blocks : Option<&Vec<String>>) -> Result<()> {
        self.documents.write().unwrap().clear();
        let mut rid = self.root_identifier.write().unwrap();
        *rid = ROOT_ID.to_string(); // Default
        drop(rid);
        self.revision_update_records.write().unwrap().clear();
        let data = self.data.read().unwrap();
        let list_str = data.list_raw_objects(DELTA_EXTENSION)?;
        drop(data);
        self.loaded_blocks.clear();
        if !list_str.is_empty() {
            for i in &list_str {
                if blocks.is_none() || blocks.unwrap().contains(i) {
                    self.load_block(i)?;
                    self.loaded_blocks.push(i.clone());
                }
            }
        }
        let mut data = self.data.write().unwrap();
        data.reload()?;
        Ok(())
    }

    /// Merges from another data structure
    pub fn merge(&mut self, other: &Melda) -> Result<()> {
        for (uuid, rt) in other.documents.read().unwrap().iter() {
            if !self.documents.read().unwrap().contains_key(uuid) {
                let rt = RevisionTree::new();
                self.documents.write().unwrap().insert(uuid.clone(), rt);
            }
            let mut docs = self.documents.write().unwrap();
            let trt = docs.get_mut(uuid).unwrap();
            trt.merge(rt);
        }
        let mut data = self.data.write().unwrap();
        let otherdata = &other.data.read().unwrap();
        data.merge(otherdata)
    }

    /// Melds the data structure into another data structure
    pub fn meld(&mut self, other: &Melda) -> Result<Vec<String>> {
        let mut result = vec![];
        let other_data = other.data.read().unwrap();
        let other_items = other_data.list_raw_objects("")?;
        if !other_items.is_empty() {
            let mut data = self.data.write().unwrap();
            let this_items = data.list_raw_objects("")?;
            let this_items: HashSet<String> = this_items.into_iter().collect();
            for i in &other_items {
                if !this_items.contains(i) {
                    data.write_raw_object(i, other_data.read_raw_object(i, 0, 0)?.as_slice())?;
                    result.push(i.clone());
                }
            }
        }
        Ok(result)
    }

    /// Reads the current state of the data structure and returns the resulting object
    pub fn read(&self) -> Result<Value> {
        let rid = self.root_identifier.read().unwrap();
        if rid.is_empty() || !self.documents.read().unwrap().contains_key(rid.as_str()) {
            bail!("no_root")
        } else {
            let c = Mutex::new(HashMap::<String, Map<String, Value>>::new());
            let docs_r = self.documents.read().unwrap();
            docs_r.par_iter().for_each(|(uuid, rt)| {
                let base_revision = rt.winner().ok_or(anyhow!("no_winner")).unwrap();
                if !base_revision.is_deleted() {
                    let data = self.data.read().unwrap();
                    let mut obj = data.read_object(base_revision, rt).unwrap();
                    obj.insert(ID_FIELD.to_string(), Value::from(uuid.clone()));
                    let mut c_w = c.lock().unwrap();
                    c_w.insert(uuid.clone(), obj);
                    drop(c_w);
                }
            });
            let c_r = c.lock().unwrap();
            let root = c_r.get(rid.as_str()).expect("root_object_not_found");
            let root = Value::from(root.clone());
            let result = Value::from(unflatten(&c_r, &root).unwrap().clone());
            drop(c_r);
            Ok(result)
        }
    }

    /// Update the data structure by processing the input object
    pub fn update(&mut self, obj: Map<String, Value>) -> Result<()> {
        let mut c = HashMap::<String, Map<String, Value>>::new();
        let path = Vec::<String>::new();
        let root = Value::from(obj);
        let root = flatten(&mut c, &root, &path);
        let root = root.as_str().expect("root_identifier_not_a_string");
        let mut rid = self.root_identifier.write().unwrap();
        *rid = root.to_owned();
        drop(rid);
        // Check for objects that have disappeared
        let mut docs_w = self.documents.write().unwrap();
        docs_w
            .par_iter_mut()
            .filter(|(uuid, _)| !c.contains_key(*uuid))
            .for_each(|(uuid, rt)| {
                let w = rt.winner().unwrap().clone();
                if !w.is_deleted() && !w.is_resolved() {
                    let rev = Revision::new_deleted(&w);
                    log::debug!("deleted {}: {} -> {}", uuid, w.to_string(), rev.to_string());
                    rt.add(rev.clone(), Some(w.clone()));
                    self.revision_update_records.write().unwrap().push((
                        uuid.clone(),
                        rev,
                        Some(w),
                    ));
                }
            });
        drop(docs_w);
        // Process updates
        c.par_iter().for_each(move |(uuid, obj)| {
            let docs_r = self.documents.read().unwrap();
            let has_rt = docs_r.contains_key(uuid);
            drop(docs_r);
            if has_rt {
                let docs_r = self.documents.read().unwrap();
                let w = docs_r.get(uuid).unwrap().winner().unwrap().clone(); // Winning revision
                drop(docs_r);
                let digest = digest_object(&obj).unwrap(); // Digest of the "full" object
                if digest.ne(&w.digest) {
                    // The w.digest corresponds to the "full" object
                    let docs_r = self.documents.read().unwrap();
                    let rt = docs_r.get(uuid).unwrap().clone();
                    drop(docs_r);
                    let data_r = self.data.read().unwrap();
                    let delta = data_r.delta_object(obj.clone(), &rt).unwrap();
                    drop(data_r);
                    if let Some(delta) = delta {
                        // The delta was created
                        let delta_digest = digest_object(&delta).unwrap();
                        let rev =
                            Revision::new_with_delta(w.index + 1, digest, delta_digest, Some(&w));
                        log::debug!("update {}: {} -> {}", uuid, w.to_string(), rev.to_string());
                        let mut docs_w = self.documents.write().unwrap();
                        let rt = docs_w.get_mut(uuid).unwrap();
                        rt.add(rev.clone(), Some(w.clone()));
                        drop(docs_w);
                        let mut data_w = self.data.write().unwrap();
                        data_w.write_object(&rev, obj.clone(), Some(delta)).unwrap();
                        drop(data_w);
                        self.revision_update_records.write().unwrap().push((
                            uuid.clone(),
                            rev,
                            Some(w),
                        ));
                    } else {
                        // There were no delta fields or the object should not be "delta-ed"
                        let rev = Revision::new(w.index + 1, digest, Some(&w));
                        log::debug!("update {}: {} -> {}", uuid, w.to_string(), rev.to_string());
                        let mut docs_w = self.documents.write().unwrap();
                        let rt = docs_w.get_mut(uuid).unwrap();
                        rt.add(rev.clone(), Some(w.clone()));
                        drop(docs_w);
                        let mut data = self.data.write().unwrap();
                        data.write_object(&rev, obj.clone(), None).unwrap();
                        self.revision_update_records.write().unwrap().push((
                            uuid.clone(),
                            rev,
                            Some(w),
                        ));
                    }
                }
            } else {
                let mut rt = RevisionTree::new();
                let rev = Revision::new(1u32, digest_object(&obj).unwrap(), None);
                log::debug!("create {}: {}", uuid, rev.to_string());
                let mut data_w = self.data.write().unwrap();
                data_w.write_object(&rev, obj.clone(), None).unwrap();
                drop(data_w);
                rt.add(rev.clone(), None);
                let mut docs_w = self.documents.write().unwrap();
                if docs_w.insert(uuid.clone(), rt).is_some() {
                    panic!("duplicate_revision_tree");
                }
                drop(docs_w);
                self.revision_update_records
                    .write()
                    .unwrap()
                    .push((uuid.clone(), rev, None));
            }
        });
        Ok(())
    }

    /// Returns the identifiers of all objects with ongoing conflicts
    pub fn in_conflict(&self) -> Vec<String> {
        let mut result = vec![];
        self.documents.read().unwrap().iter().for_each(|(d, rt)| {
            let l = rt.leafs();
            if l.len() > 1 {
                result.push(d.clone());
            }
        });
        result
    }

    /// Returns the winning revision for the given object
    pub fn winner<T>(&self, uuid: T) -> Result<String>
    where
        T: AsRef<str>,
    {
        match self.documents.read().unwrap().get(uuid.as_ref()) {
            Some(rt) => match rt.winner() {
                Some(r) => Ok(r.to_string()),
                None => Err(anyhow!("no_winner")),
            },
            None => Err(anyhow!("unknown_document")),
        }
    }

    /// Returns the conflicting revisions for the given object
    pub fn conflicting<T>(&self, uuid: T) -> Result<Vec<String>>
    where
        T: AsRef<str>,
    {
        match self.documents.read().unwrap().get(uuid.as_ref()) {
            Some(rt) => {
                let w = rt.winner().ok_or(anyhow!("no_winner"))?;
                let l = rt.leafs();
                Ok(l.iter()
                    .filter(|r| w.ne(r))
                    .map(|r| r.to_string())
                    .collect())
            }
            None => Err(anyhow!("unknown_document")),
        }
    }

    /// Resolves a conflict by choosing the new winning revision
    /// All other conflicting revisions will be marked as resolved
    pub fn resolve_as(&mut self, uuid: String, winner: &String) -> Result<()> {
        {
            let winner = Revision::from(winner).expect("invalid_revision_string");
            let docs = self.documents.read().unwrap();
            let rt = docs.get(&uuid).ok_or(anyhow!("unknown_document"))?;
            let leafs = rt.leafs();
            // We can only resolve to a valid revision
            if !leafs.contains(&winner) {
                bail!("invalid_winner_revision");
            }
            // If there is only one leaf nothing needs to be resolved
            if leafs.len() <= 1 {
                return Ok(());
            }
            // Update the winner to ensure that we do not change the view
            let data = self.data.read().unwrap();
            let merged = data.read_object(&winner, rt)?;
            drop(data);
            drop(leafs);
            drop(rt);
            drop(docs);
            self.update_object(uuid.clone(), merged)?;
        }
        let docs = self.documents.read().unwrap();
        let rt = docs.get(&uuid).ok_or(anyhow!("unknown_document"))?;
        let winner = rt.winner().expect("revision_tree_invalid_state").clone();
        // Seal all other revisions as resolved
        let mut docs = self.documents.write().unwrap();
        let rt = docs.get_mut(&uuid).ok_or(anyhow!("unknown_document"))?;
        let leafs: Vec<Revision> = rt.leafs().iter().map(|r| (*r).clone()).collect();
        for r in leafs {
            if r != winner {
                let resolved = Revision::new_resolved(&r);
                rt.add(resolved, Some(r.clone()));
            }
        }
        Ok(())
    }

    pub fn stage(&self) -> Result<Value> {
        let mut r = Map::<String, Value>::new();
        let data = self.data.read().unwrap();
        let data_stage = data.stage()?;
        r.insert(OBJECTS_FIELD.to_string(), data_stage);
        let mut revision_stage = Vec::<Value>::new();
        for (uuid, rev, prev) in self.revision_update_records.read().unwrap().iter() {
            if prev.is_none() {
                let tuple = vec![uuid.clone(), rev.digest.clone()];
                revision_stage.push(Value::from(tuple));
            } else {
                let triple = vec![
                    uuid.clone(),
                    prev.as_ref().unwrap().to_string(),
                    rev.digest.clone(),
                ];
                revision_stage.push(Value::from(triple));
            }
        }
        r.insert(CHANGESETS_FIELD.to_string(), Value::from(revision_stage));
        Ok(Value::from(r))
    }

    pub fn history(&self, uuid : &String, revision: &String) -> Result<Vec<String>> {
        let docs = self.documents.read().unwrap();
        let rt = docs.get(uuid).ok_or(anyhow!("unknown_document"))?;
        let revision = Revision::from(revision).expect("invalid_revision_string");
        let result : Vec<String> = rt.get_full_path(&revision).into_iter().map(|x| x.to_string()).collect();
        Ok(result)
    }

    pub fn parent(&self, uuid : &String, revision: &String) -> Result<Option<String>> {
        let docs = self.documents.read().unwrap();
        let rt = docs.get(uuid).ok_or(anyhow!("unknown_document"))?;
        let revision = Revision::from(revision).expect("invalid_revision_string");
        match rt.parent(&revision) {
            Some(r) => Ok(Some(r.to_string())),
            None => Ok(None),
        }
    }

    pub fn replay_stage(&mut self, s: &Value) -> Result<()> {
        if s.is_object() {
            let s = s.as_object().unwrap();
            if s.contains_key(OBJECTS_FIELD) {
                let o = s.get(OBJECTS_FIELD).unwrap();
                let mut data = self.data.write().unwrap();
                data.replay_stage(o)?;
            }
            if s.contains_key(CHANGESETS_FIELD) {
                let d = s.get(CHANGESETS_FIELD).unwrap();
                if d.is_array() {
                    for t in d.as_array().unwrap() {
                        let record = t.as_array().unwrap();
                        if record.len() == 2 {
                            let uuid =
                                record[0].as_str().ok_or(anyhow!("expecting_uuid_string"))?;
                            let digest = record[1]
                                .as_str()
                                .ok_or(anyhow!("expecting_digest_string"))?;
                            let r = Revision::new(1, digest.to_string(), None);
                            if !self.documents.read().unwrap().contains_key(uuid) {
                                let mut rt = RevisionTree::new();
                                rt.add(r, None);
                                self.documents.write().unwrap().insert(uuid.to_string(), rt);
                            } else {
                                let mut docs = self.documents.write().unwrap();
                                let rt = docs.get_mut(uuid).unwrap();
                                rt.add(r, None);
                            }
                        } else if record.len() == 3 {
                            let uuid =
                                record[0].as_str().ok_or(anyhow!("expecting_uuid_string"))?;
                            let prev = record[1]
                                .as_str()
                                .ok_or(anyhow!("expecting_revision_string"))?;
                            let digest = record[2]
                                .as_str()
                                .ok_or(anyhow!("expecting_digest_string"))?;
                            let prev = Revision::from(prev)?;
                            let r = Revision::new(prev.index + 1, digest.to_string(), Some(&prev));
                            if !self.documents.read().unwrap().contains_key(uuid) {
                                let mut rt = RevisionTree::new();
                                rt.add(r, Some(prev));
                                self.documents.write().unwrap().insert(uuid.to_string(), rt);
                            } else {
                                let mut docs = self.documents.write().unwrap();
                                let rt = docs.get_mut(uuid).unwrap();
                                rt.add(r, Some(prev));
                            }
                        } else {
                            bail!("invalid_changes_record")
                        }
                    }
                }
            }
            Ok(())
        } else {
            Err(anyhow!("expecting_stage_object"))
        }
    }
}
