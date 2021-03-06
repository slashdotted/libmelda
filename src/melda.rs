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
use crate::constants::{
    CHANGESETS_FIELD, DELTA_EXTENSION, ID_FIELD, INFORMATION_FIELD, OBJECTS_FIELD, PACK_FIELD,
    PARENTS_FIELD, ROOT_ID,
};
use crate::datastorage::DataStorage;
use crate::revision::Revision;
use crate::revisiontree::RevisionTree;
use crate::utils::{digest_bytes, digest_object, digest_string, flatten, unflatten};
use anyhow::{anyhow, bail, Result};
use rayon::prelude::*;
use serde_json::{Map, Value};
use std::collections::{BTreeMap, BTreeSet, HashMap, HashSet, VecDeque};
use std::sync::{Arc, Mutex, RwLock};

/// Change triple (used for storing block changesets)
#[derive(PartialEq, Clone)]
struct Change(String, Revision, Option<Revision>);

/// Melda is a Delta-State CRDT for arbitrary JSON documents.
pub struct Melda {
    documents: RwLock<BTreeMap<String, RevisionTree>>,
    data: RwLock<DataStorage>,
    stage: RwLock<Vec<Change>>,
    blocks: RwLock<BTreeMap<String, RwLock<Block>>>,
}

#[derive(PartialEq, Copy, Clone, Debug)]

/// Status of a cblock
enum Status {
    Unknown,
    Valid,
    ValidAndApplied, // For valid and applied blocks, changes is None
    Invalid,
}

/// Block is a public structure representing a block. It is used to represent a block that has been correctly parsed.

#[derive(Clone)]
pub struct Block {
    pub id: String,
    pub parents: Option<BTreeSet<String>>,
    pub info: Option<Map<String, Value>>,
    pub packs: Option<BTreeSet<String>>,
    changes: Option<Vec<Change>>,
    status: Status,
}

impl Melda {
    /// Initializes a new Melda data structure using the provided adapter
    ///
    /// # Arguments
    ///
    /// * `adapter` - The backend adapter used to persist the data on commit
    ///
    /// # Example
    /// ```
    /// use melda::{melda::Melda, adapter::Adapter, memoryadapter::MemoryAdapter};
    /// use std::sync::{Arc, Mutex, RwLock};
    /// use serde_json::{Map, Value,json};
    /// let adapter : Box<dyn Adapter> = Box::new(MemoryAdapter::new());
    /// let mut replica = Melda::new(Arc::new(RwLock::new(adapter))).expect("cannot_initialize_crdt");
    /// ```
    pub fn new(adapter: Arc<RwLock<Box<dyn Adapter>>>) -> Result<Melda> {
        let mut dc = Melda {
            documents: RwLock::new(BTreeMap::<String, RevisionTree>::new()),
            data: RwLock::new(DataStorage::new(adapter.clone())),
            stage: RwLock::new(Vec::<Change>::new()),
            blocks: RwLock::new(BTreeMap::new()),
        };
        dc.reload()?;
        Ok(dc)
    }

    /// Initializes a new Melda data structure using the provided Url
    ///
    /// # Arguments
    ///
    /// * `url` - The backend adapter Url used to persist the data on commit
    ///
    /// # Example
    /// ```
    /// use melda::{melda::Melda, adapter::Adapter, memoryadapter::MemoryAdapter};
    /// use std::sync::{Arc, Mutex, RwLock};
    /// use serde_json::{Map, Value,json};
    /// let mut replica = Melda::new_from_url("memory+flate://").expect("cannot_initialize_crdt");
    /// ```
    pub fn new_from_url(url: &str) -> Result<Melda> {
        let adapter = Arc::new(RwLock::new(crate::adapter::get_adapter(url).unwrap()));
        let mut dc = Melda {
            documents: RwLock::new(BTreeMap::<String, RevisionTree>::new()),
            data: RwLock::new(DataStorage::new(adapter.clone())),
            stage: RwLock::new(Vec::<Change>::new()),
            blocks: RwLock::new(BTreeMap::new()),
        };
        dc.reload()?;
        Ok(dc)
    }

    /// Initializes a new Melda data structure using the provided adapter and loads until the given block
    ///
    /// # Arguments
    ///
    /// * `adapter` - The backend adapter used to persist the data on commit
    /// * `block` - Block identifier
    ///
    /// # Example
    /// ```
    /// use melda::{melda::Melda, adapter::Adapter, memoryadapter::MemoryAdapter};
    /// use std::sync::{Arc, Mutex, RwLock};
    /// use serde_json::{Map, Value,json};
    /// let adapter : Box<dyn Adapter> = Box::new(MemoryAdapter::new());
    /// let adapter = Arc::new(RwLock::new(adapter));
    /// let mut replica = Melda::new(adapter.clone()).expect("cannot_initialize_crdt");
    /// let object = json!({ "somekey" : [ "somedata", 1u32, 2u32, 3u32, 4u32 ] }).as_object().unwrap().clone();
    /// replica.create_object("myobject", object);  
    /// assert!(replica.get_all_objects().contains("myobject"));
    /// let winner = replica.get_winner("myobject").unwrap();
    /// assert_eq!("1-e8e7db1ed2e2e9b7360c9216b8f21353e37ec0365c3d95c51a1302759da9e196", winner);
    /// let block_id = replica.commit(None).unwrap().unwrap();
    /// replica.delete_object("myobject");
    /// assert!(replica.get_all_objects().contains("myobject"));
    /// let winner = replica.get_winner("myobject").unwrap();
    /// assert_eq!("2-d_e5d1d20", winner);
    /// let value = replica.get_value("myobject", &winner);
    /// assert!(value.is_ok());
    /// assert!(value.unwrap().contains_key("_deleted"));
    /// let info = json!({ "author" : "Some user", "date" : "2022-05-23 13:47:00CET" }).as_object().unwrap().clone();
    /// replica.commit(Some(info));
    /// let mut replica = Melda::new_until(adapter, &block_id).expect("cannot_initialize_crdt");
    /// let winner = replica.get_winner("myobject").unwrap();
    /// assert_eq!("1-e8e7db1ed2e2e9b7360c9216b8f21353e37ec0365c3d95c51a1302759da9e196", winner);
    /// ```
    pub fn new_until(adapter: Arc<RwLock<Box<dyn Adapter>>>, block: &str) -> Result<Melda> {
        let mut dc = Melda {
            documents: RwLock::new(BTreeMap::<String, RevisionTree>::new()),
            data: RwLock::new(DataStorage::new(adapter.clone())),
            stage: RwLock::new(Vec::<Change>::new()),
            blocks: RwLock::new(BTreeMap::new()),
        };
        dc.reload_until(block)?;
        Ok(dc)
    }

    /// Initializes a new Melda data structure using the provided Url and loads until the given block
    ///
    /// # Arguments
    ///
    /// * `url` - The backend Url used to persist the data on commit
    /// * `block` - Block identifier
    ///
    /// ```
    pub fn new_from_url_until(url: &str, block: &str) -> Result<Melda> {
        let adapter = Arc::new(RwLock::new(crate::adapter::get_adapter(url).unwrap()));
        let mut dc = Melda {
            documents: RwLock::new(BTreeMap::<String, RevisionTree>::new()),
            data: RwLock::new(DataStorage::new(adapter.clone())),
            stage: RwLock::new(Vec::<Change>::new()),
            blocks: RwLock::new(BTreeMap::new()),
        };
        dc.reload_until(block)?;
        Ok(dc)
    }

    /// Records the creation of an object
    ///
    /// # Arguments
    ///
    /// * `uuid` - The unique identifier of the object
    /// * `obj` - The JSON object
    ///
    /// # Example
    /// ```
    /// use melda::{melda::Melda, adapter::Adapter, memoryadapter::MemoryAdapter};
    /// use std::sync::{Arc, Mutex, RwLock};
    /// use serde_json::{Map, Value,json};
    /// let adapter : Box<dyn Adapter> = Box::new(MemoryAdapter::new());
    /// let mut replica = Melda::new(Arc::new(RwLock::new(adapter))).expect("cannot_initialize_crdt");
    /// let object = json!({ "somekey" : [ "somedata", 1, 2, 3, 4 ] }).as_object().unwrap().clone();
    /// assert!(replica.create_object("myobject", object).is_ok())
    /// ```
    pub fn create_object(&mut self, uuid: &str, obj: Map<String, Value>) -> Result<()> {
        let mut rt = RevisionTree::new();
        let rev = Revision::new(1u32, digest_object(&obj)?, None);
        let mut data = self.data.write().expect("cannot_acquire_data_for_writing");
        data.write_object(&rev, obj, None)?;
        rt.add(rev.clone(), None);
        if self
            .documents
            .write()
            .expect("cannot_acquire_documents_for_writing")
            .insert(uuid.to_string(), rt)
            .is_some()
        {
            bail!("duplicate_revision_tree");
        }
        self.stage
            .write()
            .expect("cannot_acquire_stage_for_writing")
            .push(Change(uuid.to_string(), rev, None));
        Ok(())
    }

    /// Records the update of an object
    ///
    /// # Arguments
    ///
    /// * `uuid` - The unique identifier of the object
    /// * `obj` - The JSON object
    ///
    /// # Example
    /// ```
    /// use melda::{melda::Melda, adapter::Adapter, memoryadapter::MemoryAdapter};
    /// use std::sync::{Arc, Mutex, RwLock};
    /// use serde_json::{Map, Value,json};
    /// let adapter : Box<dyn Adapter> = Box::new(MemoryAdapter::new());
    /// let mut replica = Melda::new(Arc::new(RwLock::new(adapter))).expect("cannot_initialize_crdt");
    /// let object = json!({ "somekey" : [ "somedata", 1, 2, 3, 4 ] }).as_object().unwrap().clone();
    /// assert!(replica.create_object("myobject", object).is_ok());    
    /// let object = json!({ "somekey" : [ "somedata", 1, 2, 3, 4 ], "otherkey" : "otherdata" }).as_object().unwrap().clone();
    /// assert!(replica.update_object("myobject", object).is_ok());
    /// ```
    pub fn update_object(&mut self, uuid: &str, obj: Map<String, Value>) -> Result<()> {
        let docs_r = self
            .documents
            .read()
            .expect("cannot_acquire_documents_for_reading");
        match docs_r.get(uuid) {
            Some(rt) => {
                let digest = digest_object(&obj)?; // Digest of the "full" object
                let w = rt.get_winner().unwrap().clone(); // Winning revision
                if w.is_deleted() {
                    bail!("cannot_update_deleted_revision");
                } else if w.is_resolved() {
                    bail!("cannot_update_resolved_revision");
                }
                if digest.ne(&w.digest) {
                    // The w.digest corresponds to the "full" object
                    let data_r = self.data.read().expect("cannot_acquire_data_for_reading");
                    match data_r.delta_object(obj.clone(), rt)? {
                        Some(delta) => {
                            // The delta was created
                            let delta_digest = digest_object(&delta)?;
                            let rev = Revision::new_with_delta(
                                w.index + 1,
                                digest,
                                delta_digest,
                                Some(&w),
                            );
                            drop(docs_r);
                            drop(data_r);
                            self.documents
                                .write()
                                .unwrap()
                                .get_mut(uuid)
                                .unwrap()
                                .add(rev.clone(), Some(w.clone()));
                            let mut data_w =
                                self.data.write().expect("cannot_acquire_data_for_writing");
                            data_w.write_object(&rev, obj, Some(delta))?;
                            self.stage
                                .write()
                                .expect("cannot_acquire_stage_for_writing")
                                .push(Change(uuid.to_string(), rev, Some(w)));
                        }
                        None => {
                            // There were no delta fields or the object should not be "delta-ed"
                            let rev = Revision::new(w.index + 1, digest, Some(&w));
                            drop(docs_r);
                            let mut docs_w = self
                                .documents
                                .write()
                                .expect("cannot_acquire_documents_for_writing");
                            let rt = docs_w.get_mut(uuid).unwrap();
                            rt.add(rev.clone(), Some(w.clone()));
                            drop(data_r);
                            let mut data_w =
                                self.data.write().expect("cannot_acquire_data_for_writing");
                            data_w.write_object(&rev, obj, None)?;
                            self.stage
                                .write()
                                .expect("cannot_acquire_stage_for_writing")
                                .push(Change(uuid.to_string(), rev, Some(w)));
                        }
                    }
                }
                Ok(())
            }
            None => {
                drop(docs_r);
                self.create_object(uuid, obj)
            }
        }
    }

    /// Records the deletion of an object
    ///
    /// # Arguments
    ///
    /// * `uuid` - The unique identifier of the object
    ///
    /// # Example
    /// ```
    /// use melda::{melda::Melda, adapter::Adapter, memoryadapter::MemoryAdapter};
    /// use std::sync::{Arc, Mutex, RwLock};
    /// use serde_json::{Map, Value,json};
    /// let adapter : Box<dyn Adapter> = Box::new(MemoryAdapter::new());
    /// let mut replica = Melda::new(Arc::new(RwLock::new(adapter))).expect("cannot_initialize_crdt");
    /// let object = json!({ "somekey" : [ "somedata", 1, 2, 3, 4 ] }).as_object().unwrap().clone();
    /// replica.create_object("myobject", object);
    /// assert!(replica.get_all_objects().contains("myobject"));    
    /// replica.delete_object("myobject");
    /// assert!(replica.get_all_objects().contains("myobject"));
    /// let winner = replica.get_winner("myobject").unwrap();
    /// let value = replica.get_value("myobject", &winner);
    /// assert!(value.is_ok());
    /// assert!(value.unwrap().contains_key("_deleted"));
    /// ```
    pub fn delete_object(&mut self, uuid: &str) -> Result<()> {
        match self.documents.write().unwrap().get_mut(uuid) {
            Some(rt) => {
                let w = rt.get_winner().unwrap().clone();
                if !w.is_deleted() && !w.is_resolved() {
                    let rev = Revision::new_deleted(&w);
                    rt.add(rev.clone(), Some(w.clone()));
                    self.stage
                        .write()
                        .unwrap()
                        .push(Change(uuid.to_string(), rev, Some(w)));
                }
                Ok(())
            }
            None => Err(anyhow!("object_not_found")),
        }
    }

    /// Commits changes to the backend adapter
    ///
    /// # Arguments
    ///
    /// * `information` - Optional JSON object for recording additional commit information
    ///
    /// # Example
    /// ```
    /// use melda::{melda::Melda, adapter::Adapter, memoryadapter::MemoryAdapter};
    /// use std::sync::{Arc, Mutex, RwLock};
    /// use serde_json::{Map, Value,json};
    /// let adapter : Box<dyn Adapter> = Box::new(MemoryAdapter::new());
    /// let mut replica = Melda::new(Arc::new(RwLock::new(adapter))).expect("cannot_initialize_crdt");
    /// let object = json!({ "somekey" : [ "somedata", 1u32, 2u32, 3u32, 4u32 ] }).as_object().unwrap().clone();
    /// replica.create_object("myobject", object);  
    /// assert!(replica.get_all_objects().contains("myobject"));  
    /// replica.commit(None);
    /// replica.delete_object("myobject");
    /// assert!(replica.get_all_objects().contains("myobject"));
    /// let winner = replica.get_winner("myobject").unwrap();
    /// assert_eq!("2-d_e5d1d20", winner);
    /// let value = replica.get_value("myobject", &winner);
    /// assert!(value.is_ok());
    /// assert!(value.unwrap().contains_key("_deleted"));
    /// let info = json!({ "author" : "Some user", "date" : "2022-05-23 13:47:00CET" }).as_object().unwrap().clone();
    /// replica.commit(Some(info));
    /// ```
    pub fn commit(&mut self, information: Option<Map<String, Value>>) -> Result<Option<String>> {
        let stage = self.stage.read().unwrap();
        if stage.is_empty() {
            return Ok(None);
        }
        drop(stage);
        let mut block = Map::<String, Value>::new();
        let mut data = self.data.write().unwrap();
        let _packid = data.pack()?;
        // Process stage
        let mut changes = Vec::<Value>::new();
        for Change(uuid, rev, prev) in self.stage.read().unwrap().iter() {
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
        // Insert information object
        if let Some(information) = information {
            block.insert(INFORMATION_FIELD.to_string(), Value::from(information));
        }
        // Insert anchors
        let anchors_blocks = self.get_anchors();
        if !anchors_blocks.is_empty() {
            let anchors_blocks: Vec<String> =
                anchors_blocks.iter().map(|bid| bid.to_string()).collect();
            block.insert(PARENTS_FIELD.to_string(), Value::from(anchors_blocks));
        }
        // Insert pack indentifer
        if _packid.is_some() {
            let packs = vec![_packid.unwrap()];
            block.insert(PACK_FIELD.to_string(), Value::from(packs));
        }
        let blockstr = serde_json::to_string(&block).unwrap();
        let block_hash = digest_string(&blockstr);
        let blockid = block_hash.clone() + DELTA_EXTENSION;
        data.write_raw_object(&blockid, blockstr.as_bytes())?;
        // Clears the stage
        self.stage.write().unwrap().clear();
        // Load the block
        drop(data);
        let mut b = self.parse_raw_block(block_hash.clone(), block).unwrap();
        b.status = Status::ValidAndApplied;
        self.blocks
            .write()
            .unwrap()
            .insert(block_hash.clone(), RwLock::new(b));
        Ok(Some(block_hash))
    }

    /// Returns a set of the identifier of all objects
    ///
    /// # Example
    /// ```
    /// use melda::{melda::Melda, adapter::Adapter, memoryadapter::MemoryAdapter};
    /// use std::sync::{Arc, Mutex, RwLock};
    /// use serde_json::{Map, Value,json};
    /// use std::collections::BTreeSet;
    /// let adapter : Box<dyn Adapter> = Box::new(MemoryAdapter::new());
    /// let mut replica = Melda::new(Arc::new(RwLock::new(adapter))).expect("cannot_initialize_crdt");
    /// let object = json!({ "somekey" : [ "somedata", 1.0f32, 2.0f32, 3.0f32, 4.0f32 ] }).as_object().unwrap().clone();
    /// replica.create_object("myobject", object);
    /// let object = json!({ "somekey" : [ "somedata", 1.0f32, 2.0f32, 3.0f32, 4.0f32 ] }).as_object().unwrap().clone();
    /// replica.create_object("another", object);
    /// assert_eq!(replica.get_all_objects(), BTreeSet::from(["another".to_string(),"myobject".to_string()]));
    /// ```
    pub fn get_all_objects(&self) -> BTreeSet<String> {
        self.documents
            .read()
            .unwrap()
            .iter()
            .map(|(k, _)| k.clone())
            .collect()
    }

    /// Returns a the value associated with the given revision
    ///
    /// # Arguments
    ///
    /// * `uuid` - The identifier of the object
    /// * `revision`- The revision which we want to obtain the value for
    ///
    /// # Example
    /// ```
    /// use melda::{melda::Melda, adapter::Adapter, memoryadapter::MemoryAdapter};
    /// use std::sync::{Arc, Mutex, RwLock};
    /// use serde_json::{Map, Value,json};
    /// use std::collections::BTreeSet;
    /// let adapter : Box<dyn Adapter> = Box::new(MemoryAdapter::new());
    /// let mut replica = Melda::new(Arc::new(RwLock::new(adapter))).expect("cannot_initialize_crdt");
    /// let object = json!({ "somekey" : [ "somedata", 1.0f32, 2.0f32, 3.0f32, 4.0f32 ] }).as_object().unwrap().clone();
    /// replica.create_object("myobject", object.clone());
    /// let winner = replica.get_winner("myobject").unwrap();
    /// let value = replica.get_value("myobject", &winner).unwrap();
    /// assert_eq!(value, object);
    /// ```
    pub fn get_value(&self, uuid: &str, revision: &str) -> Result<Map<String, Value>> {
        let revision = Revision::from(revision).expect("invalid_revision_string");
        match self.documents.read().unwrap().get(uuid) {
            Some(o) => self.data.read().unwrap().read_object(&revision, o),
            None => Err(anyhow!("invalid object uuid")),
        }
    }

    /// Returns a set of the current anchor blocks (blocks that have not been referenced as parents)
    ///
    /// # Example
    /// ```
    /// use melda::{melda::Melda, adapter::Adapter, memoryadapter::MemoryAdapter};
    /// use std::sync::{Arc, Mutex, RwLock};
    /// use serde_json::{Map, Value,json};
    /// let adapter : Box<dyn Adapter> = Box::new(MemoryAdapter::new());
    /// let mut replica = Melda::new(Arc::new(RwLock::new(adapter))).expect("cannot_initialize_crdt");
    /// let object = json!({ "somekey" : [ "somedata", 1u32, 2u32, 3u32, 4u32 ] }).as_object().unwrap().clone();
    /// replica.create_object("myobject", object);
    /// let anchors = replica.get_anchors();
    /// assert!(anchors.is_empty());
    /// let block = replica.commit(None).unwrap().unwrap();
    /// let anchors = replica.get_anchors();
    /// assert!(anchors.len() == 1);
    /// assert!(anchors.contains(&block));
    /// ```
    pub fn get_anchors(&self) -> BTreeSet<String> {
        let blocks_r = self.blocks.read().unwrap();
        // Return the identifiers of all blocks which are not referenced as parents
        let mut anchors: BTreeSet<String> = blocks_r
            .iter()
            .filter(|(_, block)| block.read().unwrap().status == Status::ValidAndApplied)
            .map(|(k, _)| k.clone())
            .collect();
        blocks_r
            .iter()
            .filter(|(_, block)| block.read().unwrap().status == Status::ValidAndApplied)
            .for_each(|(_, b)| {
                let block_r = b.read().unwrap();
                if let Some(pr) = &block_r.parents {
                    for p in pr {
                        anchors.remove(p);
                    }
                }
            });
        anchors
    }

    /// Reloads the CRDT (reloads all delta blocks)
    ///
    /// # Example
    /// ```
    /// use melda::{melda::Melda, adapter::Adapter, memoryadapter::MemoryAdapter};
    /// use std::sync::{Arc, Mutex, RwLock};
    /// use serde_json::{Map, Value,json};
    /// let adapter : Box<dyn Adapter> = Box::new(MemoryAdapter::new());
    /// let adapter = Arc::new(RwLock::new(adapter));
    /// let mut replica = Melda::new(adapter.clone()).expect("cannot_initialize_crdt");
    /// let object = json!({ "somekey" : [ "somedata", 1u32, 2u32, 3u32, 4u32 ] }).as_object().unwrap().clone();
    /// replica.create_object("myobject", object);  
    /// assert!(replica.get_all_objects().contains("myobject"));
    /// let winner = replica.get_winner("myobject").unwrap();
    /// assert_eq!("1-e8e7db1ed2e2e9b7360c9216b8f21353e37ec0365c3d95c51a1302759da9e196", winner);
    /// let block = replica.commit(None).unwrap().unwrap();
    /// let anchors = replica.get_anchors();
    /// assert!(anchors.len() == 1);
    /// assert!(anchors.contains(&block));
    /// replica.reload();
    /// let winner = replica.get_winner("myobject").unwrap();
    /// assert_eq!("1-e8e7db1ed2e2e9b7360c9216b8f21353e37ec0365c3d95c51a1302759da9e196", winner);
    /// ```    
    pub fn reload(&mut self) -> Result<()> {
        // Check that stage is empty, otherwise fail (user must unstage explicity if necessary)
        if !self.stage.read().unwrap().is_empty() {
            bail!("stage_not_empty")
        }
        // Clear the documents
        self.documents.write().unwrap().clear();
        // Read block list
        let data = self.data.read().unwrap();
        let list_str = data.list_raw_objects(DELTA_EXTENSION)?;
        drop(data);
        self.blocks.write().unwrap().clear();
        // Reload data storage
        let mut data = self.data.write().unwrap();
        data.reload()?;
        drop(data);
        // Clear the blocks
        self.blocks.write().unwrap().clear();
        // Fetch and parse blocks
        if !list_str.is_empty() {
            for i in &list_str {
                if let Ok(block) = self.fetch_raw_block(i) {
                    if let Ok(block) = self.parse_raw_block(i.to_string(), block) {
                        self.blocks
                            .write()
                            .unwrap()
                            .insert(i.to_string(), RwLock::new(block));
                    }
                }
            }
        }
        // Mark valid blocks
        self.mark_valid_blocks();
        // Apply all valid blocks
        self.blocks.read().unwrap().iter().for_each(|(_, block)| {
            let status = block.read().unwrap().status;
            if status == Status::Valid {
                let block_r = block.read().unwrap();
                if self.apply_block(&block_r).is_ok() {
                    drop(block_r);
                    let mut block_w = block.write().unwrap();
                    block_w.status = Status::ValidAndApplied;
                    // We can drop the changes vector
                    block_w.changes = None;
                }
            }
        });
        Ok(())
    }

    /// Loads newly available blocks
    ///
    /// # Example
    /// ```
    /// use melda::{melda::Melda, adapter::Adapter, memoryadapter::MemoryAdapter};
    /// use std::sync::{Arc, Mutex, RwLock};
    /// use serde_json::{Map, Value,json};
    /// let adapter : Box<dyn Adapter> = Box::new(MemoryAdapter::new());
    /// let adapter = Arc::new(RwLock::new(adapter));
    /// let mut replica = Melda::new(adapter.clone()).expect("cannot_initialize_crdt");
    /// let object = json!({ "somekey" : [ "somedata", 1u32, 2u32, 3u32, 4u32 ] }).as_object().unwrap().clone();
    /// replica.create_object("myobject", object);  
    /// assert!(replica.get_all_objects().contains("myobject"));
    /// let winner = replica.get_winner("myobject").unwrap();
    /// assert_eq!("1-e8e7db1ed2e2e9b7360c9216b8f21353e37ec0365c3d95c51a1302759da9e196", winner);
    /// let block = replica.commit(None).unwrap().unwrap();
    /// let anchors = replica.get_anchors();
    /// assert!(anchors.len() == 1);
    /// assert!(anchors.contains(&block));
    /// replica.refresh();
    /// let winner = replica.get_winner("myobject").unwrap();
    /// assert_eq!("1-e8e7db1ed2e2e9b7360c9216b8f21353e37ec0365c3d95c51a1302759da9e196", winner);
    /// ```    
    pub fn refresh(&mut self) -> Result<()> {
        // 1. Save stage
        let stage = self.stage()?;
        // 2. Unstage
        self.unstage()?;
        // 3. Get new list of blocks
        let data_r = self.data.write().expect("cannot_acquire_data_for_writing");
        let list_str = data_r.list_raw_objects(DELTA_EXTENSION)?;
        drop(data_r);
        // 4. Refresh data storage
        let mut data_w = self.data.write().expect("cannot_acquire_data_for_writing");
        data_w.refresh()?;
        drop(data_w);
        // 5. Load new blocks
        if !list_str.is_empty() {
            for i in &list_str {
                let is_new_block = !self
                    .blocks
                    .read()
                    .expect("cannot_acquire_blocks_for_reading")
                    .contains_key(i);
                if is_new_block {
                    if let Ok(block) = self.fetch_raw_block(i) {
                        if let Ok(block) = self.parse_raw_block(i.to_string(), block) {
                            self.blocks
                                .write()
                                .expect("cannot_acquire_blocks_for_writing")
                                .insert(i.to_string(), RwLock::new(block));
                        }
                    }
                }
            }
        }
        // 6. Turn invalid blocks into unknown status blocks
        let blocks_r = self
            .blocks
            .read()
            .expect("cannot_acquire_blocks_for_reading");
        blocks_r.par_iter().for_each(|(_, block)| {
            let status = block
                .read()
                .expect("cannot_acquire_block_for_reading")
                .status;
            if status == Status::Invalid {
                block
                    .write()
                    .expect("cannot_acquire_block_for_writing")
                    .status = Status::Unknown;
            }
        });
        drop(blocks_r);
        // 7. Mark valid blocks
        self.mark_valid_blocks();
        // 8. Apply all valid blocks
        let blocks_r = self
            .blocks
            .read()
            .expect("cannot_acquire_blocks_for_reading");
        blocks_r.iter().for_each(|(_, block)| {
            let block_r = block.read().expect("cannot_acquire_block_for_reading");
            let status = block
                .read()
                .expect("cannot_acquire_block_for_reading")
                .status;
            if status == Status::Valid && self.apply_block(&block_r).is_ok() {
                drop(block_r);
                let mut block_w = block.write().expect("cannot_acquire_block_for_writing");
                block_w.status = Status::ValidAndApplied;
                // We can drop the changes vector
                block_w.changes = None;
            }
        });
        drop(blocks_r);
        // 9. Re-apply stage
        self.replay_stage(&stage)?;
        Ok(())
    }

    /// Reloads the CRDT until the given block
    ///
    /// # Arguments
    ///
    /// * `block` - Block identifier
    ///
    /// # Example
    /// ```
    /// use melda::{melda::Melda, adapter::Adapter, memoryadapter::MemoryAdapter};
    /// use std::sync::{Arc, Mutex, RwLock};
    /// use serde_json::{Map, Value,json};
    /// let adapter : Box<dyn Adapter> = Box::new(MemoryAdapter::new());
    /// let adapter = Arc::new(RwLock::new(adapter));
    /// let mut replica = Melda::new(adapter.clone()).expect("cannot_initialize_crdt");
    /// let object = json!({ "somekey" : [ "somedata", 1u32, 2u32, 3u32, 4u32 ] }).as_object().unwrap().clone();
    /// replica.create_object("myobject", object);  
    /// assert!(replica.get_all_objects().contains("myobject"));
    /// let winner = replica.get_winner("myobject").unwrap();
    /// assert_eq!("1-e8e7db1ed2e2e9b7360c9216b8f21353e37ec0365c3d95c51a1302759da9e196", winner);
    /// let block_id = replica.commit(None).unwrap().unwrap();
    /// replica.delete_object("myobject");
    /// assert!(replica.get_all_objects().contains("myobject"));
    /// let winner = replica.get_winner("myobject").unwrap();
    /// assert_eq!("2-d_e5d1d20", winner);
    /// let value = replica.get_value("myobject", &winner);
    /// assert!(value.is_ok());
    /// assert!(value.unwrap().contains_key("_deleted"));
    /// let info = json!({ "author" : "Some user", "date" : "2022-05-23 13:47:00CET" }).as_object().unwrap().clone();
    /// replica.commit(Some(info));
    /// replica.reload_until(&block_id);
    /// let winner = replica.get_winner("myobject").unwrap();
    /// assert_eq!("1-e8e7db1ed2e2e9b7360c9216b8f21353e37ec0365c3d95c51a1302759da9e196", winner);
    /// ```
    pub fn reload_until(&mut self, block_id: &str) -> Result<()> {
        let stage_r = self.stage.read().expect("cannot_acquire_stage_for_reading");
        let mut documents_w = self
            .documents
            .write()
            .expect("cannot_acquire_documents_for_writing");
        // Ensure that the stage is empty
        if !stage_r.is_empty() {
            bail!("stage_not_empty")
        }
        // Clear the documents
        documents_w.clear();
        drop(documents_w);
        // Read block list
        let data_r = self.data.write().expect("cannot_acquire_data_for_writing");
        let list_str = data_r.list_raw_objects(DELTA_EXTENSION)?;
        drop(data_r);
        // Reload data storage
        let mut data_w = self.data.write().expect("cannot_acquire_data_for_writing");
        data_w.reload()?;
        drop(data_w);
        // Clear the blocks
        let mut blocks_w = self
            .blocks
            .write()
            .expect("cannot_acquire_blocks_for_writing");
        blocks_w.clear();
        // Fetch and parse blocks
        if !list_str.is_empty() {
            for i in &list_str {
                if let Ok(block) = self.fetch_raw_block(i) {
                    if let Ok(block) = self.parse_raw_block(i.to_string(), block) {
                        blocks_w.insert(i.to_string(), RwLock::new(block));
                    }
                }
            }
        }
        drop(blocks_w);
        // Mark valid blocks
        self.mark_valid_blocks();
        // Check if block is valid
        let blocks_r = self
            .blocks
            .read()
            .expect("cannot_acquire_blocks_for_reading");
        if !blocks_r.contains_key(block_id) {
            bail!(
                "reload_until_interrupted_block_not_found: {} {:?}",
                block_id,
                blocks_r.keys()
            );
        }
        if blocks_r.get(block_id).unwrap().read().unwrap().status != Status::Valid {
            bail!("reload_until_interrupted_invalid_block: {}", block_id);
        }
        // Apply block and parents
        let mut to_apply = VecDeque::new();
        to_apply.push_back(block_id.to_string());
        while !to_apply.is_empty() {
            let bid = to_apply.pop_front().unwrap();
            let block_item = blocks_r.get(&bid).unwrap();
            let block_r = block_item.read().expect("cannot_acquire_block_for_reading");
            let status = block_r.status;
            if status == Status::Valid && self.apply_block(&block_r).is_ok() {
                if let Some(parents) = &block_r.parents {
                    for b in parents {
                        to_apply.push_back(b.to_string());
                    }
                }
                drop(block_r);
                let mut block_w = block_item
                    .write()
                    .expect("cannot_acquire_block_for_writing");
                block_w.status = Status::ValidAndApplied;
                // We can drop the changes vector
                block_w.changes = None;
            }
        }
        Ok(())
    }

    /// Drops uncommitted changes
    ///
    /// # Example
    /// ```
    /// use melda::{melda::Melda, adapter::Adapter, memoryadapter::MemoryAdapter};
    /// use std::sync::{Arc, Mutex, RwLock};
    /// use serde_json::{Map, Value,json};
    /// let adapter : Box<dyn Adapter> = Box::new(MemoryAdapter::new());
    /// let adapter = Arc::new(RwLock::new(adapter));
    /// let mut replica = Melda::new(adapter.clone()).expect("cannot_initialize_crdt");
    /// let object = json!({ "somekey" : [ "somedata", 1u32, 2u32, 3u32, 4u32 ] }).as_object().unwrap().clone();
    /// replica.create_object("myobject", object);  
    /// assert!(replica.get_all_objects().contains("myobject"));
    /// let winner = replica.get_winner("myobject").unwrap();
    /// assert_eq!("1-e8e7db1ed2e2e9b7360c9216b8f21353e37ec0365c3d95c51a1302759da9e196", winner);
    /// let block_id = replica.commit(None).unwrap().unwrap();
    /// replica.delete_object("myobject");
    /// assert!(replica.get_all_objects().contains("myobject"));
    /// let winner = replica.get_winner("myobject").unwrap();
    /// assert_eq!("2-d_e5d1d20", winner);
    /// let value = replica.get_value("myobject", &winner);
    /// assert!(value.is_ok());
    /// assert!(value.unwrap().contains_key("_deleted"));
    /// replica.unstage();
    /// let winner = replica.get_winner("myobject").unwrap();
    /// assert_eq!("1-e8e7db1ed2e2e9b7360c9216b8f21353e37ec0365c3d95c51a1302759da9e196", winner);
    /// ```
    pub fn unstage(&mut self) -> Result<()> {
        self.data.write().unwrap().unstage()?;
        let mut stage = self.stage.write().unwrap();
        let mut documents = self.documents.write().unwrap();
        stage.iter().for_each(|Change(uuid, rev, prev)| {
            if documents.contains_key(uuid) {
                let rt = documents.get_mut(uuid).unwrap();
                rt.remove(rev.clone(), prev.clone());
            }
        });
        stage.clear();
        Ok(())
    }

    /// Merges changes from another Melda into this one (does not stage changes)
    ///
    /// # Arguments
    ///
    /// * `other` - Another Melda instance
    ///
    /// # Example
    /// ```
    /// use melda::{melda::Melda, adapter::Adapter, memoryadapter::MemoryAdapter};
    /// use std::sync::{Arc, Mutex, RwLock};
    /// use serde_json::{Map, Value,json};
    /// let adapter : Box<dyn Adapter> = Box::new(MemoryAdapter::new());
    /// let adapter = Arc::new(RwLock::new(adapter));
    /// let mut replica = Melda::new(adapter.clone()).expect("cannot_initialize_crdt");
    /// let object = json!({ "somekey" : [ "somedata", 1u32, 2u32, 3u32, 4u32 ] }).as_object().unwrap().clone();
    /// replica.create_object("myobject", object);  
    /// assert!(replica.get_all_objects().contains("myobject"));
    /// let winner = replica.get_winner("myobject").unwrap();
    /// assert_eq!("1-e8e7db1ed2e2e9b7360c9216b8f21353e37ec0365c3d95c51a1302759da9e196", winner);
    /// let block_id = replica.commit(None).unwrap().unwrap();
    /// let adapter2 : Box<dyn Adapter> = Box::new(MemoryAdapter::new());
    /// let adapter2 = Arc::new(RwLock::new(adapter2));
    /// let mut replica2 = Melda::new(adapter2.clone()).expect("cannot_initialize_crdt");
    /// replica2.merge(&replica);
    /// assert!(replica2.get_all_objects().contains("myobject"));
    /// let winner = replica2.get_winner("myobject").unwrap();
    /// assert_eq!("1-e8e7db1ed2e2e9b7360c9216b8f21353e37ec0365c3d95c51a1302759da9e196", winner);
    /// assert!(replica2.get_block(&block_id).unwrap().is_none());
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

    /// Melds another Melda into this one
    ///
    /// # Arguments
    ///
    /// * `other` - Another Melda instance
    ///
    /// # Example
    /// ```
    /// use melda::{melda::Melda, adapter::Adapter, memoryadapter::MemoryAdapter};
    /// use std::sync::{Arc, Mutex, RwLock};
    /// use serde_json::{Map, Value,json};
    /// let adapter : Box<dyn Adapter> = Box::new(MemoryAdapter::new());
    /// let adapter = Arc::new(RwLock::new(adapter));
    /// let mut replica = Melda::new(adapter.clone()).expect("cannot_initialize_crdt");
    /// let object = json!({ "somekey" : [ "somedata", 1u32, 2u32, 3u32, 4u32 ] }).as_object().unwrap().clone();
    /// replica.create_object("myobject", object);  
    /// assert!(replica.get_all_objects().contains("myobject"));
    /// let winner = replica.get_winner("myobject").unwrap();
    /// assert_eq!("1-e8e7db1ed2e2e9b7360c9216b8f21353e37ec0365c3d95c51a1302759da9e196", winner);
    /// let block_id = replica.commit(None).unwrap().unwrap();
    /// let adapter2 : Box<dyn Adapter> = Box::new(MemoryAdapter::new());
    /// let adapter2 = Arc::new(RwLock::new(adapter2));
    /// let mut replica2 = Melda::new(adapter2.clone()).expect("cannot_initialize_crdt");
    /// replica2.meld(&replica);
    /// replica2.refresh();
    /// assert!(replica2.get_all_objects().contains("myobject"));
    /// let winner = replica2.get_winner("myobject").unwrap();
    /// assert_eq!("1-e8e7db1ed2e2e9b7360c9216b8f21353e37ec0365c3d95c51a1302759da9e196", winner);
    /// let block = replica2.get_block(&block_id).unwrap().unwrap();
    /// assert_eq!(block_id, block.id);
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

    /// Replicate changes from another Melda into this one
    ///
    /// # Arguments
    ///
    /// * `other` - Another Melda instance
    ///
    /// # Example
    /// ```
    /// use melda::{melda::Melda, adapter::Adapter, memoryadapter::MemoryAdapter};
    /// use std::sync::{Arc, Mutex, RwLock};
    /// use serde_json::{Map, Value,json};
    /// let adapter : Box<dyn Adapter> = Box::new(MemoryAdapter::new());
    /// let adapter = Arc::new(RwLock::new(adapter));
    /// let mut replica = Melda::new(adapter.clone()).expect("cannot_initialize_crdt");
    /// let object = json!({ "somekey" : [ "somedata", 1u32, 2u32, 3u32, 4u32 ] }).as_object().unwrap().clone();
    /// replica.create_object("myobject", object);  
    /// assert!(replica.get_all_objects().contains("myobject"));
    /// let winner = replica.get_winner("myobject").unwrap();
    /// assert_eq!("1-e8e7db1ed2e2e9b7360c9216b8f21353e37ec0365c3d95c51a1302759da9e196", winner);
    /// let block_id = replica.commit(None).unwrap().unwrap();
    /// let adapter2 : Box<dyn Adapter> = Box::new(MemoryAdapter::new());
    /// let adapter2 = Arc::new(RwLock::new(adapter2));
    /// let mut replica2 = Melda::new(adapter2.clone()).expect("cannot_initialize_crdt");
    /// replica2.replicate(&replica);
    /// assert!(replica2.get_all_objects().contains("myobject"));
    /// let winner = replica2.get_winner("myobject").unwrap();
    /// assert_eq!("1-e8e7db1ed2e2e9b7360c9216b8f21353e37ec0365c3d95c51a1302759da9e196", winner);
    /// assert!(replica2.get_block(&block_id).unwrap().is_none());
    pub fn replicate(&mut self, other: &Melda) -> Result<()> {
        let other_data = other.data.read().unwrap();
        let other_documents = other.documents.read().unwrap();
        let other_stage = other.stage.read().unwrap();
        if !self.stage.read().unwrap().is_empty() {
            bail!("stage_not_empty")
        }
        if !other_stage.is_empty() {
            bail!("other_stage_not_empty")
        }
        self.data
            .write()
            .expect("cannot_get_data_for_writing")
            .replicate(&other_data)?;
        for (d, other_rt) in other_documents.iter() {
            let mut docs_w = self
                .documents
                .write()
                .expect("cannot_get_documents_for_writing");
            if !docs_w.contains_key(d) {
                let mut rt = RevisionTree::new();
                for (rev, prev) in other_rt.get_revisions() {
                    if rt.add(rev.clone(), prev.clone()) {
                        self.stage
                            .write()
                            .expect("cannot_get_stage_for_writing")
                            .push(Change(d.clone(), rev.clone(), prev.clone()));
                    }
                }
                docs_w.insert(d.clone(), rt);
            } else {
                let rt = docs_w.get_mut(d).unwrap();
                for (rev, prev) in other_rt.get_revisions() {
                    if rt.add(rev.clone(), prev.clone()) {
                        self.stage
                            .write()
                            .expect("cannot_get_stage_for_writing")
                            .push(Change(d.clone(), rev.clone(), prev.clone()));
                    }
                }
            };
        }
        Ok(())
    }

    /// Reads the data structure and unflattens to a JSON object
    ///
    /// # Example
    /// ```
    /// use melda::{melda::Melda, adapter::Adapter, memoryadapter::MemoryAdapter};
    /// use std::sync::{Arc, Mutex, RwLock};
    /// use serde_json::{Map, Value,json};
    /// let adapter : Box<dyn Adapter> = Box::new(MemoryAdapter::new());
    /// let adapter = Arc::new(RwLock::new(adapter));
    /// let mut replica = Melda::new(adapter.clone()).expect("cannot_initialize_crdt");
    /// let object = json!({ "somekey" : [ "somedata", 1u32, 2u32, 3u32, 4u32 ] }).as_object().unwrap().clone();
    /// replica.update(object.clone());
    /// let readback = replica.read().unwrap();
    /// assert!(readback.contains_key("somekey"));
    pub fn read(&self) -> Result<Map<String, Value>> {
        if !self.documents.read().unwrap().contains_key(ROOT_ID) {
            bail!("no_root")
        } else {
            let c = Mutex::new(HashMap::<String, Map<String, Value>>::new());
            let docs_r = self.documents.read().unwrap();
            docs_r.par_iter().for_each(|(uuid, rt)| {
                let base_revision = rt.get_winner().ok_or_else(|| anyhow!("no_winner")).unwrap();
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
            let root = c_r.get(ROOT_ID).expect("root_object_not_found");
            let root = Value::from(root.clone());
            let result = unflatten(&c_r, &root)
                .unwrap()
                .as_object()
                .expect("not_an_object")
                .clone();
            drop(c_r);
            Ok(result)
        }
    }

    /// Updates the data structure by flattening the input JSON object
    ///
    /// # Arguments
    ///
    /// * `obj` - input JSON object
    ///
    /// # Example
    /// ```
    /// use melda::{melda::Melda, adapter::Adapter, memoryadapter::MemoryAdapter};
    /// use std::sync::{Arc, Mutex, RwLock};
    /// use serde_json::{Map, Value,json};
    /// let adapter : Box<dyn Adapter> = Box::new(MemoryAdapter::new());
    /// let adapter = Arc::new(RwLock::new(adapter));
    /// let mut replica = Melda::new(adapter.clone()).expect("cannot_initialize_crdt");
    /// let object = json!({ "somekey" : [ "somedata", 1u32, 2u32, 3u32, 4u32 ] }).as_object().unwrap().clone();
    /// replica.update(object.clone());
    /// let readback = replica.read().unwrap();
    /// assert!(readback.contains_key("somekey"));
    pub fn update(&mut self, obj: Map<String, Value>) -> Result<()> {
        let mut c = HashMap::<String, Map<String, Value>>::new();
        let path = Vec::<String>::new();
        let root = Value::from(obj);
        let root = flatten(&mut c, &root, &path);
        let root = root.as_str().expect("root_identifier_not_a_string");
        if root != ROOT_ID {
            bail!("invalid_root_id");
        }
        // Check for objects that have disappeared
        let mut docs_w = self.documents.write().unwrap();
        docs_w
            .par_iter_mut()
            .filter(|(uuid, _)| !c.contains_key(*uuid))
            .for_each(|(uuid, rt)| {
                let w = rt.get_winner().unwrap().clone();
                if !w.is_deleted() && !w.is_resolved() {
                    let rev = Revision::new_deleted(&w);
                    rt.add(rev.clone(), Some(w.clone()));
                    self.stage
                        .write()
                        .unwrap()
                        .push(Change(uuid.clone(), rev, Some(w)));
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
                let w = docs_r.get(uuid).unwrap().get_winner().unwrap().clone(); // Winning revision
                drop(docs_r);
                let digest = digest_object(obj).unwrap(); // Digest of the "full" object
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
                        let mut docs_w = self.documents.write().unwrap();
                        let rt = docs_w.get_mut(uuid).unwrap();
                        rt.add(rev.clone(), Some(w.clone()));
                        drop(docs_w);
                        let mut data_w = self.data.write().unwrap();
                        data_w.write_object(&rev, obj.clone(), Some(delta)).unwrap();
                        drop(data_w);
                        self.stage
                            .write()
                            .unwrap()
                            .push(Change(uuid.clone(), rev, Some(w)));
                    } else {
                        // There were no delta fields or the object should not be "delta-ed"
                        let rev = Revision::new(w.index + 1, digest, Some(&w));
                        let mut docs_w = self.documents.write().unwrap();
                        let rt = docs_w.get_mut(uuid).unwrap();
                        rt.add(rev.clone(), Some(w.clone()));
                        drop(docs_w);
                        let mut data = self.data.write().unwrap();
                        data.write_object(&rev, obj.clone(), None).unwrap();
                        self.stage
                            .write()
                            .unwrap()
                            .push(Change(uuid.clone(), rev, Some(w)));
                    }
                }
            } else {
                let mut rt = RevisionTree::new();
                let rev = Revision::new(1u32, digest_object(obj).unwrap(), None);
                let mut data_w = self.data.write().unwrap();
                data_w.write_object(&rev, obj.clone(), None).unwrap();
                drop(data_w);
                rt.add(rev.clone(), None);
                let mut docs_w = self.documents.write().unwrap();
                if docs_w.insert(uuid.clone(), rt).is_some() {
                    panic!("duplicate_revision_tree");
                }
                drop(docs_w);
                self.stage
                    .write()
                    .unwrap()
                    .push(Change(uuid.clone(), rev, None));
            }
        });
        Ok(())
    }

    /// Returns a set of the object (identifiers) which have ongoing conflicts
    ///
    /// # Example
    /// ```
    /// use melda::{melda::Melda, adapter::Adapter, memoryadapter::MemoryAdapter};
    /// use std::sync::{Arc, Mutex, RwLock};
    /// use serde_json::{Map, Value,json};
    /// let adapter : Box<dyn Adapter> = Box::new(MemoryAdapter::new());
    /// let adapter = Arc::new(RwLock::new(adapter));
    /// let mut replica = Melda::new(adapter.clone()).expect("cannot_initialize_crdt");
    /// let object = json!({ "somekey" : [ "somedata", 1u32, 2u32, 3u32, 4u32 ] }).as_object().unwrap().clone();
    /// replica.create_object("myobject", object);  
    /// assert!(replica.get_all_objects().contains("myobject"));
    /// let winner = replica.get_winner("myobject").unwrap();
    /// assert_eq!("1-e8e7db1ed2e2e9b7360c9216b8f21353e37ec0365c3d95c51a1302759da9e196", winner);
    /// let block_id = replica.commit(None).unwrap().unwrap();
    /// let adapter2 : Box<dyn Adapter> = Box::new(MemoryAdapter::new());
    /// let adapter2 = Arc::new(RwLock::new(adapter2));
    /// let mut replica2 = Melda::new(adapter2.clone()).expect("cannot_initialize_crdt");
    /// let object = json!({ "another" : [ "somedata", 1u32, 2u32, 3u32, 4u32 ] }).as_object().unwrap().clone();
    /// replica2.create_object("myobject", object);  
    /// replica2.meld(&replica);
    /// replica2.refresh();
    /// let conflicting = replica2.in_conflict();
    /// assert!(conflicting.contains("myobject"));
    pub fn in_conflict(&self) -> BTreeSet<String> {
        let mut result = BTreeSet::new();
        self.documents.read().unwrap().iter().for_each(|(d, rt)| {
            let l = rt.get_leafs();
            if l.len() > 1 {
                result.insert(d.clone());
            }
        });
        result
    }

    /// Returns the winning revision for the given object
    ///
    /// # Arguments
    ///
    /// * `uuid` - The uuid of the object
    ///
    /// # Example
    /// ```
    /// use melda::{melda::Melda, adapter::Adapter, memoryadapter::MemoryAdapter};
    /// use std::sync::{Arc, Mutex, RwLock};
    /// use serde_json::{Map, Value,json};
    /// let adapter : Box<dyn Adapter> = Box::new(MemoryAdapter::new());
    /// let adapter = Arc::new(RwLock::new(adapter));
    /// let mut replica = Melda::new(adapter.clone()).expect("cannot_initialize_crdt");
    /// let object = json!({ "somekey" : [ "somedata", 1u32, 2u32, 3u32, 4u32 ] }).as_object().unwrap().clone();
    /// replica.create_object("myobject", object);  
    /// assert!(replica.get_all_objects().contains("myobject"));
    /// let winner = replica.get_winner("myobject").unwrap();
    /// assert_eq!("1-e8e7db1ed2e2e9b7360c9216b8f21353e37ec0365c3d95c51a1302759da9e196", winner);
    /// let block_id = replica.commit(None).unwrap().unwrap();
    /// replica.delete_object("myobject");
    /// assert!(replica.get_all_objects().contains("myobject"));
    /// let winner = replica.get_winner("myobject").unwrap();
    /// assert_eq!("2-d_e5d1d20", winner);
    /// let value = replica.get_value("myobject", &winner);
    /// assert!(value.is_ok());
    /// assert!(value.unwrap().contains_key("_deleted"));
    /// replica.unstage();
    /// let winner = replica.get_winner("myobject").unwrap();
    /// assert_eq!("1-e8e7db1ed2e2e9b7360c9216b8f21353e37ec0365c3d95c51a1302759da9e196", winner);
    /// ```
    pub fn get_winner<T>(&self, uuid: T) -> Result<String>
    where
        T: AsRef<str>,
    {
        match self.documents.read().unwrap().get(uuid.as_ref()) {
            Some(rt) => match rt.get_winner() {
                Some(r) => Ok(r.to_string()),
                None => Err(anyhow!("no_winner")),
            },
            None => Err(anyhow!("unknown_document")),
        }
    }

    /// Returns a set of the conflicting revisions of the given object (the winning revision is not included!)
    ///
    /// # Arguments
    ///
    /// * `uuid` - The uuid of the object
    ///
    /// # Example
    /// ```
    /// use melda::{melda::Melda, adapter::Adapter, memoryadapter::MemoryAdapter};
    /// use std::sync::{Arc, Mutex, RwLock};
    /// use serde_json::{Map, Value,json};
    /// let adapter : Box<dyn Adapter> = Box::new(MemoryAdapter::new());
    /// let adapter = Arc::new(RwLock::new(adapter));
    /// let mut replica = Melda::new(adapter.clone()).expect("cannot_initialize_crdt");
    /// let object = json!({ "somekey" : [ "somedata", 1u32, 2u32, 3u32, 4u32 ] }).as_object().unwrap().clone();
    /// replica.create_object("myobject", object);  
    /// assert!(replica.get_all_objects().contains("myobject"));
    /// let winner = replica.get_winner("myobject").unwrap();
    /// assert_eq!("1-e8e7db1ed2e2e9b7360c9216b8f21353e37ec0365c3d95c51a1302759da9e196", winner);
    /// let block_id = replica.commit(None).unwrap().unwrap();
    /// let adapter2 : Box<dyn Adapter> = Box::new(MemoryAdapter::new());
    /// let adapter2 = Arc::new(RwLock::new(adapter2));
    /// let mut replica2 = Melda::new(adapter2.clone()).expect("cannot_initialize_crdt");
    /// let object = json!({ "another" : [ "somedata", 1u32, 2u32, 3u32, 4u32 ] }).as_object().unwrap().clone();
    /// replica2.create_object("myobject", object);
    /// let winner2 = replica2.get_winner("myobject").unwrap();
    /// replica2.meld(&replica);
    /// replica2.refresh();
    /// let conflicting = replica2.in_conflict();
    /// assert!(conflicting.contains("myobject"));
    /// let revs = replica2.get_conflicting("myobject").unwrap();
    /// assert!(revs.contains(&winner2));
    pub fn get_conflicting<T>(&self, uuid: T) -> Result<BTreeSet<String>>
    where
        T: AsRef<str>,
    {
        match self.documents.read().unwrap().get(uuid.as_ref()) {
            Some(rt) => {
                let w = rt.get_winner().ok_or_else(|| anyhow!("no_winner"))?;
                let l = rt.get_leafs();
                Ok(l.iter()
                    .filter(|r| w.ne(r))
                    .map(|r| r.to_string())
                    .collect())
            }
            None => Err(anyhow!("unknown_document")),
        }
    }

    /// Resolves a conflict by choosing the new winning revision. All other conflicting revisions are marked as resolved.
    ///
    /// # Arguments
    ///
    /// * `uuid` - The uuid of the object
    /// * `winner` - The revision that is to be made the winner
    ///
    /// # Example
    /// ```
    /// use melda::{melda::Melda, adapter::Adapter, memoryadapter::MemoryAdapter};
    /// use std::sync::{Arc, Mutex, RwLock};
    /// use serde_json::{Map, Value,json};
    /// let adapter : Box<dyn Adapter> = Box::new(MemoryAdapter::new());
    /// let adapter = Arc::new(RwLock::new(adapter));
    /// let mut replica = Melda::new(adapter.clone()).expect("cannot_initialize_crdt");
    /// let object = json!({ "somekey" : [ "somedata", 1u32, 2u32, 3u32, 4u32 ] }).as_object().unwrap().clone();
    /// replica.create_object("myobject", object);  
    /// assert!(replica.get_all_objects().contains("myobject"));
    /// let winner = replica.get_winner("myobject").unwrap();
    /// assert_eq!("1-e8e7db1ed2e2e9b7360c9216b8f21353e37ec0365c3d95c51a1302759da9e196", winner);
    /// let block_id = replica.commit(None).unwrap().unwrap();
    /// let adapter2 : Box<dyn Adapter> = Box::new(MemoryAdapter::new());
    /// let adapter2 = Arc::new(RwLock::new(adapter2));
    /// let mut replica2 = Melda::new(adapter2.clone()).expect("cannot_initialize_crdt");
    /// let object = json!({ "another" : [ "somedata", 1u32, 2u32, 3u32, 4u32 ] }).as_object().unwrap().clone();
    /// replica2.create_object("myobject", object);
    /// let winner2 = replica2.get_winner("myobject").unwrap();
    /// assert_eq!("1-255cc6219e48f526c04bc5af86439c34e4fe39fcdc611758ff833a2ff80583f0", winner2);
    /// replica2.meld(&replica);
    /// replica2.refresh();
    /// let conflicting = replica2.in_conflict();
    /// assert!(conflicting.contains("myobject"));
    /// let revs = replica2.get_conflicting("myobject").unwrap();
    /// assert!(revs.contains(&winner2));
    /// replica2.resolve_as("myobject", &winner2);
    /// let winner = replica2.get_winner("myobject").unwrap();
    /// assert_eq!("2-255cc6219e48f526c04bc5af86439c34e4fe39fcdc611758ff833a2ff80583f0_e5d1d20", winner);
    pub fn resolve_as(&mut self, uuid: &str, winner: &str) -> Result<String> {
        {
            let winner = Revision::from(winner).expect("invalid_revision_string");
            let docs_r = self.documents.read().unwrap();
            let rt = docs_r
                .get(uuid)
                .ok_or_else(|| anyhow!("unknown_document"))?;
            let leafs = rt.get_leafs();
            // We can only resolve to a status revision
            if !leafs.contains(&winner) {
                bail!("invalid_winner_revision");
            }
            // If there is only one leaf nothing needs to be resolved
            if leafs.len() <= 1 {
                bail!("not_in_conflict");
            }
            // Update the winner to ensure that we do not change the view
            let data_r = self.data.read().unwrap();
            let merged = data_r.read_object(&winner, rt)?;
            drop(data_r);
            drop(leafs);
            drop(docs_r);
            self.update_object(uuid, merged)?;
        }
        let docs_r = self.documents.read().unwrap();
        let rt = docs_r
            .get(uuid)
            .ok_or_else(|| anyhow!("unknown_document"))?;
        let winner = rt
            .get_winner()
            .expect("revision_tree_invalid_state")
            .clone();
        drop(docs_r);
        // Seal all other revisions as resolved
        let mut docs_w = self.documents.write().unwrap();
        let rt = docs_w
            .get_mut(uuid)
            .ok_or_else(|| anyhow!("unknown_document"))?;
        let leafs: Vec<Revision> = rt.get_leafs().iter().map(|r| (*r).clone()).collect();
        for r in leafs {
            if r != winner {
                let resolved = Revision::new_resolved(&r);
                if rt.add(resolved.clone(), Some(r.clone())) {
                    self.stage.write().unwrap().push(Change(
                        uuid.to_string(),
                        resolved,
                        Some(r.clone()),
                    ));
                }
            }
        }
        Ok(winner.to_string())
    }

    /// Saves the current stage
    ///
    /// # Example
    /// ```
    /// use melda::{melda::Melda, adapter::Adapter, memoryadapter::MemoryAdapter};
    /// use std::sync::{Arc, Mutex, RwLock};
    /// use serde_json::{Map, Value,json};
    /// let adapter : Box<dyn Adapter> = Box::new(MemoryAdapter::new());
    /// let adapter = Arc::new(RwLock::new(adapter));
    /// let mut replica = Melda::new(adapter.clone()).expect("cannot_initialize_crdt");
    /// let object = json!({ "somekey" : [ "somedata", 1u32, 2u32, 3u32, 4u32 ] }).as_object().unwrap().clone();
    /// replica.create_object("myobject", object);  
    /// assert!(replica.get_all_objects().contains("myobject"));
    /// let winner = replica.get_winner("myobject").unwrap();
    /// assert_eq!("1-e8e7db1ed2e2e9b7360c9216b8f21353e37ec0365c3d95c51a1302759da9e196", winner);
    /// let block_id = replica.commit(None).unwrap().unwrap();
    /// replica.delete_object("myobject");
    /// assert!(replica.get_all_objects().contains("myobject"));
    /// let winner = replica.get_winner("myobject").unwrap();
    /// assert_eq!("2-d_e5d1d20", winner);
    /// let value = replica.get_value("myobject", &winner);
    /// assert!(value.is_ok());
    /// assert!(value.unwrap().contains_key("_deleted"));
    /// let stage = replica.stage().unwrap();
    /// replica.unstage();
    /// let winner = replica.get_winner("myobject").unwrap();
    /// assert_eq!("1-e8e7db1ed2e2e9b7360c9216b8f21353e37ec0365c3d95c51a1302759da9e196", winner);
    /// replica.replay_stage(&stage);
    /// let winner = replica.get_winner("myobject").unwrap();
    /// assert_eq!("2-d_e5d1d20", winner);
    /// ```
    pub fn stage(&self) -> Result<Option<Value>> {
        let mut r = Map::<String, Value>::new();
        let data = self.data.read().unwrap();
        let data_stage = data.stage()?;
        r.insert(OBJECTS_FIELD.to_string(), data_stage);
        let rur = self.stage.read().unwrap();
        if !rur.is_empty() {
            let mut changes = Vec::<Value>::new();
            for Change(uuid, rev, prev) in rur.iter() {
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
            r.insert(CHANGESETS_FIELD.to_string(), Value::from(changes));
            Ok(Some(Value::from(r)))
        } else {
            Ok(None)
        }
    }

    /// Replays a stage
    ///
    /// # Arguments
    ///
    /// * `s` - The stage to be replayed
    ///
    /// # Example
    /// ```
    /// use melda::{melda::Melda, adapter::Adapter, memoryadapter::MemoryAdapter};
    /// use std::sync::{Arc, Mutex, RwLock};
    /// use serde_json::{Map, Value,json};
    /// let adapter : Box<dyn Adapter> = Box::new(MemoryAdapter::new());
    /// let adapter = Arc::new(RwLock::new(adapter));
    /// let mut replica = Melda::new(adapter.clone()).expect("cannot_initialize_crdt");
    /// let object = json!({ "somekey" : [ "somedata", 1u32, 2u32, 3u32, 4u32 ] }).as_object().unwrap().clone();
    /// replica.create_object("myobject", object);  
    /// assert!(replica.get_all_objects().contains("myobject"));
    /// let winner = replica.get_winner("myobject").unwrap();
    /// assert_eq!("1-e8e7db1ed2e2e9b7360c9216b8f21353e37ec0365c3d95c51a1302759da9e196", winner);
    /// let block_id = replica.commit(None).unwrap().unwrap();
    /// replica.delete_object("myobject");
    /// assert!(replica.get_all_objects().contains("myobject"));
    /// let winner = replica.get_winner("myobject").unwrap();
    /// assert_eq!("2-d_e5d1d20", winner);
    /// let value = replica.get_value("myobject", &winner);
    /// assert!(value.is_ok());
    /// assert!(value.unwrap().contains_key("_deleted"));
    /// let stage = replica.stage().unwrap();
    /// replica.unstage();
    /// let winner = replica.get_winner("myobject").unwrap();
    /// assert_eq!("1-e8e7db1ed2e2e9b7360c9216b8f21353e37ec0365c3d95c51a1302759da9e196", winner);
    /// replica.replay_stage(&stage);
    /// let winner = replica.get_winner("myobject").unwrap();
    /// assert_eq!("2-d_e5d1d20", winner);
    /// ```
    pub fn replay_stage(&mut self, s: &Option<Value>) -> Result<()> {
        if let Some(s) = s {
            if s.is_object() {
                let s = s.as_object().unwrap();
                if s.contains_key(OBJECTS_FIELD) {
                    let o = s.get(OBJECTS_FIELD).unwrap();
                    let mut data = self.data.write().unwrap();
                    data.replay_stage(o)?;
                }
                if s.contains_key(CHANGESETS_FIELD) {
                    let changes = s.get(CHANGESETS_FIELD);
                    if let Some(changes) = changes {
                        if changes.is_array() {
                            for c in changes.as_array().unwrap() {
                                if c.is_array() {
                                    let record = c.as_array().unwrap();
                                    if record.len() == 2 {
                                        let uuid = record[0]
                                            .as_str()
                                            .ok_or_else(|| anyhow!("expecting_uuid_string"))?;
                                        let digest = record[1]
                                            .as_str()
                                            .ok_or_else(|| anyhow!("expecting_digest_string"))?;
                                        let r = Revision::new(1, digest.to_string(), None);
                                        if !self.documents.read().unwrap().contains_key(uuid) {
                                            let mut rt = RevisionTree::new();
                                            if rt.add(r.clone(), None) {
                                                self.stage.write().unwrap().push(Change(
                                                    uuid.to_string(),
                                                    r,
                                                    None,
                                                ));
                                            }
                                            self.documents
                                                .write()
                                                .unwrap()
                                                .insert(uuid.to_string(), rt);
                                        } else {
                                            let mut docs = self.documents.write().unwrap();
                                            let rt = docs.get_mut(uuid).unwrap();
                                            if rt.add(r.clone(), None) {
                                                self.stage.write().unwrap().push(Change(
                                                    uuid.to_string(),
                                                    r,
                                                    None,
                                                ));
                                            }
                                        }
                                    } else if record.len() == 3 {
                                        let uuid = record[0]
                                            .as_str()
                                            .ok_or_else(|| anyhow!("expecting_uuid_string"))?;
                                        let prev = record[1]
                                            .as_str()
                                            .ok_or_else(|| anyhow!("expecting_revision_string"))?;
                                        let digest = record[2]
                                            .as_str()
                                            .ok_or_else(|| anyhow!("expecting_digest_string"))?;
                                        let prev = Revision::from(prev)?;
                                        let r = Revision::new(
                                            prev.index + 1,
                                            digest.to_string(),
                                            Some(&prev),
                                        );
                                        if !self.documents.read().unwrap().contains_key(uuid) {
                                            // FIXME: Should this be allowed?
                                            // This might happen if we save the stage, then reload to a previous block
                                            // were an object did not yet exist and then try to re-apply the stage
                                            let mut rt = RevisionTree::new();
                                            self.stage.write().unwrap().push(Change(
                                                uuid.to_string(),
                                                r.clone(),
                                                Some(prev.clone()),
                                            ));
                                            rt.add(r, Some(prev));
                                            self.documents
                                                .write()
                                                .unwrap()
                                                .insert(uuid.to_string(), rt);
                                        } else {
                                            let mut docs = self.documents.write().unwrap();
                                            let rt = docs.get_mut(uuid).unwrap();
                                            if rt.add(r.clone(), Some(prev.clone())) {
                                                self.stage.write().unwrap().push(Change(
                                                    uuid.to_string(),
                                                    r,
                                                    Some(prev),
                                                ));
                                            }
                                        }
                                    } else if record.len() == 4 {
                                        let uuid = record[0]
                                            .as_str()
                                            .ok_or_else(|| anyhow!("expecting_uuid_string"))?;
                                        let prev = record[1]
                                            .as_str()
                                            .ok_or_else(|| anyhow!("expecting_revision_string"))?;
                                        let digest = record[2]
                                            .as_str()
                                            .ok_or_else(|| anyhow!("expecting_digest_string"))?;
                                        let delta_digest = record[3]
                                            .as_str()
                                            .ok_or_else(|| anyhow!("expecting_digest_string"))?;
                                        let prev = Revision::from(prev)?;
                                        let r = Revision::new_with_delta(
                                            prev.index + 1,
                                            digest.to_string(),
                                            delta_digest.to_string(),
                                            Some(&prev),
                                        );
                                        if !self.documents.read().unwrap().contains_key(uuid) {
                                            // FIXME: Should this be allowed?
                                            // This might happen if we save the stage, then reload to a previous block
                                            // were an object did not yet exist and then try to re-apply the stage
                                            let mut rt = RevisionTree::new();
                                            self.stage.write().unwrap().push(Change(
                                                uuid.to_string(),
                                                r.clone(),
                                                Some(prev.clone()),
                                            ));
                                            rt.add(r, Some(prev));
                                            self.documents
                                                .write()
                                                .unwrap()
                                                .insert(uuid.to_string(), rt);
                                        } else {
                                            let mut docs = self.documents.write().unwrap();
                                            let rt = docs.get_mut(uuid).unwrap();
                                            if rt.add(r.clone(), Some(prev.clone())) {
                                                self.stage.write().unwrap().push(Change(
                                                    uuid.to_string(),
                                                    r,
                                                    Some(prev),
                                                ));
                                            }
                                        }
                                    } else {
                                        bail!("invalid_changes_record")
                                    }
                                }
                            }
                        }
                    }
                }
                Ok(())
            } else {
                Err(anyhow!("expecting_stage_object"))
            }
        } else {
            Ok(())
        }
    }

    /// Returns a block, or None if the block does not exist.
    ///
    /// # Arguments
    ///
    /// * `block_id` - Block identifier
    ///
    /// # Example
    /// ```
    /// use melda::{melda::Melda, adapter::Adapter, memoryadapter::MemoryAdapter};
    /// use std::sync::{Arc, Mutex, RwLock};
    /// use serde_json::{Map, Value,json};
    /// let adapter : Box<dyn Adapter> = Box::new(MemoryAdapter::new());
    /// let adapter = Arc::new(RwLock::new(adapter));
    /// let mut replica = Melda::new(adapter.clone()).expect("cannot_initialize_crdt");
    /// let object = json!({ "somekey" : [ "somedata", 1u32, 2u32, 3u32, 4u32 ] }).as_object().unwrap().clone();
    /// replica.create_object("myobject", object);  
    /// let winner = replica.get_winner("myobject").unwrap();
    /// let parent = replica.get_parent_revision("myobject", &winner).unwrap();
    /// assert!(parent.is_none());
    /// let block_id = replica.commit(None).unwrap().unwrap();
    /// let block = replica.get_block(&block_id).unwrap().unwrap();
    /// assert_eq!(block_id, block.id);
    pub fn get_block(&self, block_id: &str) -> Result<Option<Block>> {
        let blocks_r = self
            .blocks
            .read()
            .expect("cannot_acquire_blocks_for_reading");
        match blocks_r.get(block_id) {
            Some(b) => {
                let block_r = b.read().expect("cannot_acquire_block_for_reading");
                Ok(Some(block_r.clone()))
            }
            None => Ok(None),
        }
    }

    /// Returns the parent revision in the revision tree of the specified object, or None if there is no parent
    ///
    /// # Arguments
    ///
    /// * `uuid` - Object identifier
    /// * `revision` - The revision
    ///
    /// # Example
    /// ```
    /// use melda::{melda::Melda, adapter::Adapter, memoryadapter::MemoryAdapter};
    /// use std::sync::{Arc, Mutex, RwLock};
    /// use serde_json::{Map, Value,json};
    /// let adapter : Box<dyn Adapter> = Box::new(MemoryAdapter::new());
    /// let adapter = Arc::new(RwLock::new(adapter));
    /// let mut replica = Melda::new(adapter.clone()).expect("cannot_initialize_crdt");
    /// let object = json!({ "somekey" : [ "somedata", 1u32, 2u32, 3u32, 4u32 ] }).as_object().unwrap().clone();
    /// replica.create_object("myobject", object);  
    /// let winner = replica.get_winner("myobject").unwrap();
    /// let parent = replica.get_parent_revision("myobject", &winner).unwrap();
    /// assert!(parent.is_none());
    /// let block_id = replica.commit(None).unwrap().unwrap();
    /// replica.delete_object("myobject");
    /// let newrev = replica.get_winner("myobject").unwrap();
    /// assert_eq!("2-d_e5d1d20", newrev);
    /// let parent = replica.get_parent_revision("myobject", &newrev).unwrap().unwrap();
    /// assert_eq!(&parent, &winner);
    pub fn get_parent_revision(&self, uuid: &str, revision: &str) -> Result<Option<String>> {
        let docs = self.documents.read().unwrap();
        let rt = docs.get(uuid).ok_or_else(|| anyhow!("unknown_document"))?;
        let revision = Revision::from(revision).expect("invalid_revision_string");
        match rt.get_parent(&revision) {
            Some(r) => Ok(Some(r.to_string())),
            None => Ok(None),
        }
    }

    // Fetch a block and verify digest
    fn fetch_raw_block(&self, blockid: &str) -> Result<Map<String, Value>> {
        let object = blockid.to_string() + DELTA_EXTENSION;
        let data = self.data.read().unwrap();
        let data = data.read_raw_object(object.as_str(), 0, 0)?;
        let digest = digest_bytes(data.as_slice());
        if !digest.eq(blockid) {
            bail!("mismatching_block_hash");
        }
        let json = std::str::from_utf8(&data)?;
        let json: Value = serde_json::from_str(json)?;
        if !json.is_object() {
            bail!("invalid_block_format");
        }
        let blockobj = json.as_object().unwrap();
        Ok(blockobj.clone())
    }

    /// Parse a block
    fn parse_raw_block(&self, b_id: String, raw_block: Map<String, Value>) -> Result<Block> {
        // Block values
        let mut b_parents: Option<BTreeSet<String>> = None;
        let mut b_info: Option<Map<String, Value>> = None;
        let mut b_packs: Option<BTreeSet<String>> = None;
        let mut b_changes: Option<Vec<Change>> = None;
        // Parse raw block fields
        if raw_block.contains_key(CHANGESETS_FIELD) {
            if raw_block.contains_key(PACK_FIELD) {
                let packs = raw_block
                    .get(PACK_FIELD)
                    .ok_or_else(|| anyhow!("missing_pack_reference"))
                    .unwrap()
                    .as_array()
                    .ok_or_else(|| anyhow!("packs_not_an_array"))?;
                if !packs.iter().all(|x| {
                    if x.is_string() {
                        let data = self.data.read().unwrap();
                        data.is_readable_and_valid_pack(x.as_str().unwrap())
                            .unwrap_or(false)
                    } else {
                        false
                    }
                }) {
                    bail!("missing_packs");
                }
                // Collect identifiers
                if !packs.is_empty() {
                    b_packs = Some(
                        packs
                            .iter()
                            .map(|p| p.as_str().unwrap().to_string())
                            .collect(),
                    );
                }
            }
            if raw_block.contains_key(INFORMATION_FIELD) {
                let info = raw_block
                    .get(INFORMATION_FIELD)
                    .ok_or_else(|| anyhow!("missing_root_id"))?;
                if !info.is_object() {
                    bail!("info_not_an_object");
                }
                // Save identifier
                b_info = Some(info.as_object().unwrap().clone());
            }
            if raw_block.contains_key(PARENTS_FIELD) {
                let parents = raw_block
                    .get(PARENTS_FIELD)
                    .ok_or_else(|| anyhow!("missing_parents_field"))?;
                if !parents.is_array() {
                    bail!("parents_not_an_array");
                }
                let mut ps = BTreeSet::new();
                for p in parents.as_array().unwrap() {
                    if p.is_string() {
                        ps.insert(p.as_str().unwrap().to_string());
                    }
                }
                // Save parents
                if !ps.is_empty() {
                    b_parents = Some(ps);
                }
            }
            let changes = raw_block.get(CHANGESETS_FIELD);
            if let Some(changes) = changes {
                if changes.is_array() {
                    // Process changeset
                    let mut cs: Vec<Change> = vec![];
                    for c in changes.as_array().unwrap() {
                        if c.is_array() {
                            let record = c.as_array().unwrap();
                            if record.len() == 2 {
                                // Creation record
                                let uuid = record[0]
                                    .as_str()
                                    .ok_or_else(|| anyhow!("expecting_uuid_string"))?;
                                let digest = record[1]
                                    .as_str()
                                    .ok_or_else(|| anyhow!("expecting_digest_string"))?;
                                let r = Revision::new(1, digest.to_string(), None);
                                cs.push(Change(uuid.to_string(), r, None));
                            } else if record.len() == 3 {
                                // Update record
                                let uuid = record[0]
                                    .as_str()
                                    .ok_or_else(|| anyhow!("expecting_uuid_string"))?;
                                let prev = record[1]
                                    .as_str()
                                    .ok_or_else(|| anyhow!("expecting_revision_string"))?;
                                let digest = record[2]
                                    .as_str()
                                    .ok_or_else(|| anyhow!("expecting_digest_string"))?;
                                let prev = Revision::from(prev)?;
                                let r =
                                    Revision::new(prev.index + 1, digest.to_string(), Some(&prev));
                                cs.push(Change(uuid.to_string(), r, Some(prev)));
                            } else if record.len() == 4 {
                                // Update record with delta
                                let uuid = record[0]
                                    .as_str()
                                    .ok_or_else(|| anyhow!("expecting_uuid_string"))?;
                                let prev = record[1]
                                    .as_str()
                                    .ok_or_else(|| anyhow!("expecting_revision_string"))?;
                                let digest = record[2]
                                    .as_str()
                                    .ok_or_else(|| anyhow!("expecting_digest_string"))?;
                                let delta_digest = record[3]
                                    .as_str()
                                    .ok_or_else(|| anyhow!("expecting_digest_string"))?;
                                let prev = Revision::from(prev)?;
                                let r = Revision::new_with_delta(
                                    prev.index + 1,
                                    digest.to_string(),
                                    delta_digest.to_string(),
                                    Some(&prev),
                                );
                                cs.push(Change(uuid.to_string(), r, Some(prev)));
                            } else {
                                bail!("invalid_changes_record")
                            }
                        }
                    }
                    if !cs.is_empty() {
                        b_changes = Some(cs);
                    }
                }
            }
        }
        Ok(Block {
            id: b_id,
            parents: b_parents,
            info: b_info,
            packs: b_packs,
            changes: b_changes,
            status: Status::Unknown,
        })
    }

    fn check_block(&self, bid: &str) -> Status {
        let blocks = self.blocks.read().unwrap();
        let data = self.data.read().unwrap();
        let packs = data.get_loaded_packs();
        if let Some(block) = blocks.get(bid) {
            // If the block status has been determined return the corresponding value
            let mut status = block.read().unwrap().status;
            if status != Status::Unknown {
                return status;
            }
            // Verify that all packs are available
            status = Status::Valid;
            if let Some(pks) = &block.read().unwrap().packs {
                if !pks.par_iter().all(|pack| packs.contains(pack)) {
                    status = Status::Invalid;
                }
            };
            if status == Status::Valid {
                // Verify that all parent blocks are status
                if let Some(parents) = &block.read().unwrap().parents {
                    if !parents
                        .iter()
                        .all(|parent| self.check_block(parent) != Status::Invalid)
                    {
                        status = Status::Invalid;
                    }
                };
            }
            block.write().unwrap().status = status;
            status
        } else {
            Status::Invalid
        }
    }

    fn mark_valid_blocks(&self) {
        let blocks = self.blocks.read().unwrap();
        blocks.iter().for_each(|(bid, block)| {
            let status = block.read().unwrap().status;
            if status == Status::Unknown {
                self.check_block(bid);
            }
        });
    }

    fn apply_block(&self, block: &Block) -> Result<()> {
        if let Some(changes) = &block.changes {
            for change in changes {
                let Change(uuid, r, prev) = change;
                if !self.documents.read().unwrap().contains_key(uuid) {
                    let mut rt = RevisionTree::new();
                    rt.add(r.clone(), prev.clone());
                    self.documents.write().unwrap().insert(uuid.to_string(), rt);
                } else {
                    let mut docs = self.documents.write().unwrap();
                    let rt = docs.get_mut(uuid).unwrap();
                    rt.add(r.clone(), prev.clone());
                }
            }
        };
        Ok(())
    }
}
