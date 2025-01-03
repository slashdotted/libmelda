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
use crate::constants::{
    ARRAY_DESCRIPTOR_DELTA_ORDER_FIELD, ARRAY_DESCRIPTOR_ORDER_FIELD, CHANGESETS_FIELD,
    DELTA_EXTENSION, ID_FIELD, INFORMATION_FIELD, OBJECTS_FIELD, PACK_FIELD, PARENTS_FIELD,
    ROOT_ID,
};
use crate::datastorage::DataStorage;
use crate::revision::Revision;
use crate::revisiontree::RevisionTree;
use crate::utils::{
    apply_diff_patch, digest_bytes, digest_object, digest_string, flatten, is_array_descriptor,
    make_diff_patch, merge_arrays, unflatten,
};
use anyhow::{anyhow, bail, Result};
use lru::LruCache;
use rayon::prelude::*;
use serde_json::{Map, Value};
use std::collections::{BTreeMap, BTreeSet, HashMap, HashSet, VecDeque};
use std::num::NonZeroUsize;
use std::sync::{Arc, Mutex, RwLock};

/// Change triple (used for storing block changesets)
#[derive(PartialEq, Clone)]
struct Change(String, Revision, Option<Revision>);

/// Melda is a Delta-State CRDT for arbitrary JSON documents.
pub struct Melda {
    documents: RwLock<BTreeMap<String, Mutex<RevisionTree>>>,
    data: RwLock<DataStorage>,
    blocks: RwLock<BTreeMap<String, RwLock<Block>>>,
    array_descriptors_cache: Mutex<LruCache<Revision, ArrayDescriptor>>,
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

// Array descriptor represents an array descriptor. It is used to support reconstruction of delta descriptors
#[derive(Clone)]
struct ArrayDescriptor {
    patch: Option<Vec<Value>>,
    order: Option<Vec<Value>>,
}

impl ArrayDescriptor {
    // Constructs a new array descriptor by parsing the provided JSON object
    pub fn new_from_object(object: Map<String, Value>) -> Result<ArrayDescriptor> {
        if let Some(field) = object.get(ARRAY_DESCRIPTOR_ORDER_FIELD) {
            if let Some(array) = field.as_array() {
                Ok(ArrayDescriptor {
                    patch: None,
                    order: Some(array.clone()),
                })
            } else {
                Err(anyhow!("order_field_is_not_an_array"))
            }
        } else if let Some(field) = object.get(ARRAY_DESCRIPTOR_DELTA_ORDER_FIELD) {
            if let Some(array) = field.as_array() {
                Ok(ArrayDescriptor {
                    patch: Some(array.clone()),
                    order: None,
                })
            } else {
                Err(anyhow!("delta_order_field_is_not_an_array"))
            }
        } else {
            // If the object corresponds to a deleted or resolved revision generate an
            // empty order
            if let Some(field) = object.get("_deleted") {
                if let Some(v) = field.as_bool() {
                    if v {
                        Ok(ArrayDescriptor {
                            patch: None,
                            order: Some(vec![]),
                        })
                    } else {
                        Err(anyhow!("malformed_deleted_array_descriptor_false"))
                    }
                } else {
                    Err(anyhow!("malformed_deleted_array_descriptor"))
                }
            } else if let Some(field) = object.get("_resolved") {
                if let Some(v) = field.as_bool() {
                    if v {
                        Ok(ArrayDescriptor {
                            patch: None,
                            order: Some(vec![]),
                        })
                    } else {
                        Err(anyhow!("malformed_resolved_array_descriptor_false"))
                    }
                } else {
                    Err(anyhow!("malformed_resolved_array_descriptor"))
                }
            } else {
                Err(anyhow!("malformed_array_descriptor"))
            }
        }
    }

    pub fn new_from_order(order: Vec<Value>) -> ArrayDescriptor {
        ArrayDescriptor {
            patch: None,
            order: Some(order),
        }
    }

    pub fn new_from_patch(diff: Vec<Value>) -> ArrayDescriptor {
        ArrayDescriptor {
            patch: Some(diff),
            order: None,
        }
    }

    pub fn to_json_object(&self) -> Map<String, Value> {
        let mut o = Map::<String, Value>::new();
        if self.is_diff() {
            o.insert(
                ARRAY_DESCRIPTOR_DELTA_ORDER_FIELD.to_string(),
                Value::from(self.patch.clone().unwrap()),
            );
        } else {
            o.insert(
                ARRAY_DESCRIPTOR_ORDER_FIELD.to_string(),
                Value::from(self.order.clone().unwrap()),
            );
        };
        o
    }

    pub fn is_diff(&self) -> bool {
        self.patch.is_some()
    }

    pub fn get_patch(&self) -> &Option<Vec<Value>> {
        &self.patch
    }

    pub fn get_order(&self) -> &Option<Vec<Value>> {
        &self.order
    }
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
        let cache_size = std::env::var("MELDA_ARRAYDESCRIPTORS_CACHE_CAP")
            .unwrap_or_else(|_| "16".to_string())
            .parse::<u32>()
            .unwrap() as usize;
        let dc = Melda {
            documents: RwLock::new(BTreeMap::<String, Mutex<RevisionTree>>::new()),
            data: RwLock::new(DataStorage::new(adapter.clone())),
            blocks: RwLock::new(BTreeMap::new()),
            array_descriptors_cache: Mutex::new(LruCache::<Revision, ArrayDescriptor>::new(
                NonZeroUsize::new(cache_size).unwrap(),
            )),
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
        let cache_size = std::env::var("MELDA_ARRAYDESCRIPTORS_CACHE_CAP")
            .unwrap_or_else(|_| "16".to_string())
            .parse::<u32>()
            .unwrap() as usize;
        let adapter = Arc::new(RwLock::new(crate::adapter::get_adapter(url).unwrap()));
        let dc = Melda {
            documents: RwLock::new(BTreeMap::<String, Mutex<RevisionTree>>::new()),
            data: RwLock::new(DataStorage::new(adapter.clone())),
            blocks: RwLock::new(BTreeMap::new()),
            array_descriptors_cache: Mutex::new(LruCache::<Revision, ArrayDescriptor>::new(
                NonZeroUsize::new(cache_size).unwrap(),
            )),
        };
        dc.reload()?;
        Ok(dc)
    }

    /// Returns the underlying storage adapter
    pub fn get_adapter(&self) -> Arc<RwLock<Box<dyn Adapter>>> {
        let data = self.data.read().expect("cannot_acquire_data_for_reading");
        data.get_adapter()
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
    /// let committed_anchors = replica.commit(None).unwrap().unwrap();
    /// replica.delete_object("myobject");
    /// assert!(replica.get_all_objects().contains("myobject"));
    /// let winner = replica.get_winner("myobject").unwrap();
    /// assert_eq!("2-d_e5d1d20", winner);
    /// let value = replica.get_value("myobject", Some(&winner));
    /// assert!(value.is_ok());
    /// assert!(value.unwrap().contains_key("_deleted"));
    /// let info = json!({ "author" : "Some user", "date" : "2022-05-23 13:47:00CET" }).as_object().unwrap().clone();
    /// replica.commit(Some(info));
    /// let mut replica = Melda::new_until(adapter, &committed_anchors).expect("cannot_initialize_crdt");
    /// let winner = replica.get_winner("myobject").unwrap();
    /// assert_eq!("1-e8e7db1ed2e2e9b7360c9216b8f21353e37ec0365c3d95c51a1302759da9e196", winner);
    /// ```
    pub fn new_until(
        adapter: Arc<RwLock<Box<dyn Adapter>>>,
        anchors: &BTreeSet<String>,
    ) -> Result<Melda> {
        let cache_size = std::env::var("MELDA_ARRAYDESCRIPTORS_CACHE_CAP")
            .unwrap_or_else(|_| "16".to_string())
            .parse::<u32>()
            .unwrap() as usize;
        let dc = Melda {
            documents: RwLock::new(BTreeMap::<String, Mutex<RevisionTree>>::new()),
            data: RwLock::new(DataStorage::new(adapter.clone())),
            blocks: RwLock::new(BTreeMap::new()),
            array_descriptors_cache: Mutex::new(LruCache::<Revision, ArrayDescriptor>::new(
                NonZeroUsize::new(cache_size).unwrap(),
            )),
        };
        dc.reload_until(anchors)?;
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
    pub fn new_from_url_until(url: &str, anchors: &BTreeSet<String>) -> Result<Melda> {
        let cache_size = std::env::var("MELDA_ARRAYDESCRIPTORS_CACHE_CAP")
            .unwrap_or_else(|_| "16".to_string())
            .parse::<u32>()
            .unwrap() as usize;
        let adapter = Arc::new(RwLock::new(crate::adapter::get_adapter(url).unwrap()));
        let dc = Melda {
            documents: RwLock::new(BTreeMap::<String, Mutex<RevisionTree>>::new()),
            data: RwLock::new(DataStorage::new(adapter.clone())),
            blocks: RwLock::new(BTreeMap::new()),
            array_descriptors_cache: Mutex::new(LruCache::<Revision, ArrayDescriptor>::new(
                NonZeroUsize::new(cache_size).unwrap(),
            )),
        };
        dc.reload_until(anchors)?;
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
    /// let result = replica.create_object("myobject", object.clone());
    /// assert!(result.is_ok());
    /// assert_eq!(result.unwrap().unwrap(), "1-e8e7db1ed2e2e9b7360c9216b8f21353e37ec0365c3d95c51a1302759da9e196");
    /// let result = replica.create_object("myobject", object);
    /// assert!(result.is_ok());
    /// assert!(result.unwrap().is_none());
    /// ```
    pub fn create_object(&self, uuid: &str, obj: Map<String, Value>) -> Result<Option<String>> {
        // Create initial revision
        let rev = Revision::new(
            1u32,
            digest_object(&obj).expect("cannot_create_revision"),
            None,
        );
        let mut data_w = self.data.write().expect("cannot_acquire_data_for_writing");
        data_w.write_object(&rev, obj).expect("cannot_write_object");
        drop(data_w);
        // Obtain the revision tree (either an existing one of a new one)
        let mut docs_w = self
            .documents
            .write()
            .expect("cannot_acquire_documents_for_writing");
        let rt_w = docs_w
            .entry(uuid.to_string())
            .or_insert_with(|| Mutex::new(RevisionTree::new()))
            .get_mut()
            .expect("cannot_acquire_revision_tree_for_writing");
        if rt_w.add(rev.clone(), None, true) {
            drop(docs_w);
            Ok(Some(rev.to_string()))
        } else {
            drop(docs_w);
            Ok(None)
        }
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
    /// let result = replica.update_object("myobject", object);
    /// assert!(result.is_ok());
    /// assert_eq!(result.unwrap().unwrap(), "2-9e84b4db64036b29b7ad7def2efa95a11e1ffe93e6e5cf56e93b07ef8d3976ff_e5d1d20");
    /// let object2 = json!({ "somekey" : [ "somedata", 1, 2, 3, 4 ], "otherkey" : "otherdata" }).as_object().unwrap().clone();
    /// let result = replica.update_object("myobject2", object2);
    /// assert!(result.is_ok());
    /// assert_eq!(result.unwrap().unwrap(), "1-9e84b4db64036b29b7ad7def2efa95a11e1ffe93e6e5cf56e93b07ef8d3976ff");
    /// ```
    pub fn update_object(&self, uuid: &str, obj: Map<String, Value>) -> Result<Option<String>> {
        // Obtain the revision tree (either an existing one of a new one)
        let docs_r = self
            .documents
            .read()
            .expect("cannot_acquire_documents_for_reading");
        if let Some(rt) = docs_r.get(uuid) {
            // Existing object
            let mut rt_w = rt.lock().expect("cannot_acquire_revision_tree_for_writing");
            if let Some(winning_revision) = rt_w.get_winner() {
                // If its an array descriptor first need to compute the delta
                // If create_delta_array_descriptor returns None it means that there are
                // no differences between the current array and the new one
                let object = if is_array_descriptor(uuid) {
                    self.create_delta_array_descriptor(obj, &rt_w).unwrap()
                } else {
                    Some(obj)
                };
                // Now compute the digest to see if the object has changed
                // An object can be None if its an "empty" delta array descriptor
                if let Some(object) = object {
                    let digest = digest_object(&object).unwrap(); // Digest of the current object
                    if digest.ne(winning_revision.digest()) {
                        // Digest is different, there was an update
                        let rev = Revision::new_updated(digest, winning_revision);
                        let winning_revision = winning_revision.clone();
                        rt_w.add(rev.clone(), Some(winning_revision.clone()), true);
                        let mut data_w =
                            self.data.write().expect("cannot_acquire_data_for_writing");
                        data_w.write_object(&rev, object).unwrap();
                        drop(data_w);
                        Ok(Some(rev.to_string()))
                    } else {
                        Ok(None)
                    }
                } else {
                    Err(anyhow!("invalid_object"))
                }
            } else {
                Err(anyhow!("object_has_no_winner"))
            }
        } else {
            // Newly created object
            drop(docs_r);
            // No winning revision, assume that its a new object
            self.create_object(uuid, obj)
        }
    }

    fn read_object_at_revision(
        &self,
        uuid: &str,
        rt: &RevisionTree,
        rev: &Revision,
    ) -> Result<Map<String, Value>> {
        if is_array_descriptor(uuid) {
            let order = self
                .get_merged_order_at_revision(rt, rev)
                .expect("cannot_get_merged_order");
            Ok(ArrayDescriptor::new_from_order(order).to_json_object())
        } else {
            Ok(self
                .data
                .read()
                .expect("cannot_acquire_data_for_reading")
                .read_object(rev)
                .expect("cannot_read_object"))
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
    /// let value = replica.get_value("myobject", Some(&winner));
    /// assert!(value.is_ok());
    /// assert!(value.unwrap().contains_key("_deleted"));
    /// let adapter : Box<dyn Adapter> = Box::new(MemoryAdapter::new());
    /// let adapter = Arc::new(RwLock::new(adapter));
    /// let mut replica = Melda::new(adapter.clone()).expect("cannot_initialize_crdt");
    /// let object = json!({ "somekey\u{266D}" : { "_id": "1", "key" : "alpha" }}).as_object().unwrap().clone();
    /// replica.update(object.clone());
    /// let readback = replica.read().unwrap();
    /// let content = serde_json::to_string(&readback).unwrap();
    /// assert_eq!("{\"_id\":\"\u{221A}\",\"somekey\u{266D}\":{\"_id\":\"1\",\"key\":\"alpha\"}}", content);
    /// let result = replica.delete_object("1");
    /// assert!(result.is_ok());
    /// assert_eq!(result.unwrap().unwrap(), "2-d_5423aab");
    /// let readback = replica.read().unwrap();
    /// let content = serde_json::to_string(&readback).unwrap();
    /// assert_eq!("{\"_id\":\"\u{221A}\",\"somekey\u{266D}\":null}", content);
    /// let result2 = replica.delete_object("xxxx");
    /// assert!(result2.is_ok());
    /// assert!(result2.unwrap().is_none());
    /// ```
    pub fn delete_object(&self, uuid: &str) -> Result<Option<String>> {
        let docs_r = self
            .documents
            .read()
            .expect("cannot_acquire_documents_for_reading");
        if let Some(rt) = docs_r.get(uuid) {
            let mut rt_w = rt.lock().expect("cannot_acquire_revision_tree_for_writing");
            if let Some(winning_revision) = rt_w.get_winner() {
                if !winning_revision.is_deleted() && !winning_revision.is_resolved() {
                    let rev = Revision::new_deleted(winning_revision);
                    let winning_revision = winning_revision.clone();
                    rt_w.add(rev.clone(), Some(winning_revision.clone()), true);
                    Ok(Some(rev.to_string()))
                } else {
                    Ok(None)
                }
            } else {
                Err(anyhow!("object_has_no_winner"))
            }
        } else {
            Ok(None)
        }
    }

    /// Records the deletion of an object if it has a committed history, delete it
    /// completely if there is no committed history
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
    /// replica.create_object("myobject", object.clone());
    /// assert!(replica.get_all_objects().contains("myobject"));    
    /// replica.remove_object("myobject");
    /// assert!(!replica.get_all_objects().contains("myobject"));
    /// replica.create_object("myobject", object);
    /// replica.commit(None);
    /// let winner = replica.get_winner("myobject").unwrap();
    /// let value = replica.get_value("myobject", Some(&winner));
    /// assert!(value.is_ok());
    /// let result2 = replica.remove_object("xxxx");
    /// assert!(result2.is_ok());
    /// assert!(result2.unwrap().is_none());
    /// ```
    pub fn remove_object(&self, uuid: &str) -> Result<Option<String>> {
        let docs_r = self
            .documents
            .read()
            .expect("cannot_acquire_documents_for_reading");
        if let Some(rt) = docs_r.get(uuid) {
            let mut rt_w = rt.lock().expect("cannot_acquire_revision_tree_for_writing");
            rt_w.unstage();
            if rt_w.is_empty() {
                drop(rt_w);
                drop(docs_r);
                let mut docs_w = self
                    .documents
                    .write()
                    .expect("cannot_acquire_documents_for_writing");
                docs_w.remove(uuid);
                Ok(None)
            } else if let Some(winning_revision) = rt_w.get_winner() {
                if !winning_revision.is_deleted() && !winning_revision.is_resolved() {
                    let rev = Revision::new_deleted(winning_revision);
                    let winning_revision = winning_revision.clone();
                    rt_w.add(rev.clone(), Some(winning_revision.clone()), true);
                    Ok(Some(rev.to_string()))
                } else {
                    Ok(None)
                }
            } else {
                Err(anyhow!("object_has_no_winner"))
            }
        } else {
            Ok(None)
        }
    }

    // TODO: Maybe we could use the same logic for commit and stage / apply_block and replay_stage
    // for in both we create or apply a block
    // in commit we need to clear the stage afterwards and we need to write the pack and the block

    /// Commits changes to the backend adapter and returns the set of anchors (containing the identifier
    /// of the committed block)
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
    /// let value = replica.get_value("myobject", Some(&winner));
    /// assert!(value.is_ok());
    /// assert!(value.unwrap().contains_key("_deleted"));
    /// let info = json!({ "author" : "Some user", "date" : "2022-05-23 13:47:00CET" }).as_object().unwrap().clone();
    /// replica.commit(Some(info));
    /// assert!(replica.get_all_objects().contains("myobject"));
    /// let winner2 = replica.get_winner("myobject").unwrap();
    /// assert_eq!("2-d_e5d1d20", winner2);
    /// let value2 = replica.get_value("myobject", Some(&winner));
    /// assert!(value2.is_ok());
    /// assert!(value2.unwrap().contains_key("_deleted"));
    /// ```
    pub fn commit(
        &self,
        information: Option<Map<String, Value>>,
    ) -> Result<Option<BTreeSet<String>>> {
        // Automatically resolve conflicts in array_descriptors
        for (uuid, rt) in self.documents.read().unwrap().iter() {
            if is_array_descriptor(uuid) {
                let rt_r = rt.lock().expect("cannot_acquire_revision_tree_for_commit");
                let w = rt_r.get_winner().ok_or_else(|| anyhow!("no_winner"))?;
                let l = rt_r.get_leafs();
                if l.len() > 1 {
                    self.resolve_as(uuid, w.to_string().as_str())
                        .expect("cannot_automatically_resolve_array_descriptor_conflict");
                }
            }
        }
        // Commit data packs
        let mut block = Map::<String, Value>::new();
        let mut data: std::sync::RwLockWriteGuard<'_, DataStorage> =
            self.data.write().expect("cannot_acquire_data_for_writing");
        let _packid = data.pack()?;
        // Process stage
        let mut changes = Vec::<Value>::new();
        for (uuid, rt) in self.documents.read().unwrap().iter() {
            let rt_rw = rt.lock().expect("cannot_acquire_revision_tree_for_commit");
            if rt_rw.has_staging() {
                rt_rw.get_revisions().iter().for_each(|(rev, rte)| {
                    if rte.is_staging() {
                        if rte.get_parent().is_none() {
                            // Creation record
                            let tuple = vec![uuid.clone(), rev.digest().clone()];
                            changes.push(Value::from(tuple));
                        } else {
                            // Update record
                            let triple = vec![
                                uuid.clone(),
                                rte.get_parent().as_ref().unwrap().to_string(),
                                rev.digest().clone(),
                            ];
                            changes.push(Value::from(triple));
                        }
                    }
                })
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
        data.write_raw_item(&blockid, blockstr.as_bytes())?;
        // Load the block
        drop(data);
        let mut b = self.parse_raw_block(block_hash.clone(), block).unwrap();
        b.status = Status::ValidAndApplied;
        self.blocks
            .write()
            .unwrap()
            .insert(block_hash.clone(), RwLock::new(b));
        // Commit changes
        for (_, rt) in self.documents.read().unwrap().iter() {
            let mut rt_rw = rt.lock().expect("cannot_acquire_revision_tree_for_commit");
            rt_rw.commit();
        }
        let anchors = BTreeSet::from([block_hash]);
        Ok(Some(anchors))
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
    /// let value = replica.get_value("myobject", Some(&winner)).unwrap();
    /// assert_eq!(value, object);
    /// let value = replica.get_value("myobject", None).unwrap();
    /// assert_eq!(value, object);
    /// ```
    pub fn get_value(&self, uuid: &str, revision: Option<&str>) -> Result<Map<String, Value>> {
        match revision {
            Some(revision) => {
                let revision = Revision::from(revision).expect("invalid_revision_string");
                match self
                    .documents
                    .read()
                    .expect("failed_to_acquire_documents_for_reading")
                    .get(uuid)
                {
                    Some(rt) => {
                        let rt_r = rt
                            .lock()
                            .expect("failed_to_acquire_revision_tree_for_reading");
                        if !rt_r.get_revisions().contains_key(&revision) {
                            Err(anyhow!("invalid object revision"))
                        } else {
                            self.data
                                .read()
                                .expect("cannot_acquire_data_for_reading")
                                .read_object(&revision)
                        }
                    }
                    None => Err(anyhow!("invalid object uuid")),
                }
            }
            None => {
                match self
                    .documents
                    .read()
                    .expect("failed_to_acquire_documents_for_reading")
                    .get(uuid)
                {
                    Some(rt) => {
                        let rt_r = rt
                            .lock()
                            .expect("failed_to_acquire_revision_tree_for_reading");
                        let revision = rt_r.get_winner().expect("object_has_no_winner");
                        self.data
                            .read()
                            .expect("cannot_acquire_data_for_reading")
                            .read_object(revision)
                    }
                    None => Err(anyhow!("invalid object uuid")),
                }
            }
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
    /// let committed_anchors = replica.commit(None).unwrap().unwrap();
    /// let anchors = replica.get_anchors();
    /// assert!(committed_anchors.len() == 1);
    /// assert!(anchors.len() == 1);
    /// assert!(anchors == committed_anchors);
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
    /// let committed_anchors = replica.commit(None).unwrap().unwrap();
    /// let anchors = replica.get_anchors();
    /// assert!(anchors.len() == 1);
    /// assert!(anchors == committed_anchors);
    /// replica.reload();
    /// let winner = replica.get_winner("myobject").unwrap();
    /// assert_eq!("1-e8e7db1ed2e2e9b7360c9216b8f21353e37ec0365c3d95c51a1302759da9e196", winner);
    /// ```    
    pub fn reload(&self) -> Result<()> {
        // Check that stage is empty, otherwise fail (user must unstage explicity if necessary)
        if self.has_staging() {
            bail!("stage_not_empty")
        }
        // Clear the documents
        self.documents
            .write()
            .expect("failed_to_acquire_documents_for_writing")
            .clear();
        // Read block list
        let data = self.data.read().expect("cannot_acquire_data_for_reading");
        let list_str = data.list_raw_items(DELTA_EXTENSION)?;
        drop(data);
        self.blocks.write().unwrap().clear();
        // Reload data storage
        let mut data = self.data.write().expect("cannot_acquire_data_for_writing");
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
    /// let committed_anchors = replica.commit(None).unwrap().unwrap();
    /// let anchors = replica.get_anchors();
    /// assert!(anchors.len() == 1);
    /// assert!(anchors == committed_anchors);
    /// replica.refresh();
    /// let winner = replica.get_winner("myobject").unwrap();
    /// assert_eq!("1-e8e7db1ed2e2e9b7360c9216b8f21353e37ec0365c3d95c51a1302759da9e196", winner);
    /// ```    
    pub fn refresh(&mut self) -> Result<()> {
        // Check that stage is empty, otherwise fail (user must unstage explicity if necessary)
        if self.has_staging() {
            bail!("stage_not_empty")
        }
        // 1. Get new list of blocks
        let data_r = self.data.read().expect("cannot_acquire_data_for_writing");
        let list_str = data_r.list_raw_items(DELTA_EXTENSION)?;
        drop(data_r);
        // 2. Refresh data storage
        let mut data_w = self.data.write().expect("cannot_acquire_data_for_writing");
        data_w.refresh()?;
        drop(data_w);
        // 3. Load new blocks
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
        // 4. Turn invalid blocks into unknown status blocks
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
        // 5. Mark valid blocks
        self.mark_valid_blocks();
        // 6. Apply all valid blocks
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
    /// let committed_anchors = replica.commit(None).unwrap().unwrap();
    /// replica.delete_object("myobject");
    /// assert!(replica.get_all_objects().contains("myobject"));
    /// let winner = replica.get_winner("myobject").unwrap();
    /// assert_eq!("2-d_e5d1d20", winner);
    /// let value = replica.get_value("myobject", Some(&winner));
    /// assert!(value.is_ok());
    /// assert!(value.unwrap().contains_key("_deleted"));
    /// let info = json!({ "author" : "Some user", "date" : "2022-05-23 13:47:00CET" }).as_object().unwrap().clone();
    /// replica.commit(Some(info));
    /// replica.reload_until(&committed_anchors);
    /// let winner = replica.get_winner("myobject").unwrap();
    /// assert_eq!("1-e8e7db1ed2e2e9b7360c9216b8f21353e37ec0365c3d95c51a1302759da9e196", winner);
    /// ```
    pub fn reload_until(&self, anchors: &BTreeSet<String>) -> Result<()> {
        if anchors.is_empty() {
            return self.reload();
        }
        // Ensure that the stage is empty
        if self.has_staging() {
            bail!("stage_not_empty")
        }
        let mut documents_w = self
            .documents
            .write()
            .expect("cannot_acquire_documents_for_writing");
        // Clear the documents
        documents_w.clear();
        drop(documents_w);
        // Read block list
        let data_r = self.data.write().expect("cannot_acquire_data_for_writing");
        let list_str = data_r.list_raw_items(DELTA_EXTENSION)?;
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
        // Check if blocks are valid
        let blocks_r = self
            .blocks
            .read()
            .expect("cannot_acquire_blocks_for_reading");
        for block_id in anchors {
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
        }
        // Apply block and parents
        let mut to_apply = VecDeque::new();
        for block_id in anchors {
            to_apply.push_back(block_id.to_string());
        }
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
    /// let value = replica.get_value("myobject", Some(&winner));
    /// assert!(value.is_ok());
    /// assert!(value.unwrap().contains_key("_deleted"));
    /// replica.unstage();
    /// let winner = replica.get_winner("myobject").unwrap();
    /// assert_eq!("1-e8e7db1ed2e2e9b7360c9216b8f21353e37ec0365c3d95c51a1302759da9e196", winner);
    /// ```
    pub fn unstage(&mut self) -> Result<()> {
        self.data
            .write()
            .expect("cannot_acquire_data_for_writing")
            .unstage()?;
        let mut docs_w = self
            .documents
            .write()
            .expect("failed_to_acquire_documents_for_writing");
        docs_w.par_iter_mut().for_each(|(_, rt_w)| {
            rt_w.get_mut()
                .expect("cannot_acquire_revision_tree_for_writing")
                .unstage()
        });
        docs_w.retain(|_, rt| {
            !rt.get_mut()
                .expect("cannot_acquire_revision_tree_for_reading")
                .is_empty()
        });
        Ok(())
    }

    /// Melds another Melda into this one. Only committed items (delta blocks and data packs) are melded.
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
    /// let committed_anchors = replica.commit(None).unwrap().unwrap();
    /// let adapter2 : Box<dyn Adapter> = Box::new(MemoryAdapter::new());
    /// let adapter2 = Arc::new(RwLock::new(adapter2));
    /// let mut replica2 = Melda::new(adapter2.clone()).expect("cannot_initialize_crdt");
    /// replica2.meld(&replica);
    /// replica2.refresh();
    /// assert!(replica2.get_all_objects().contains("myobject"));
    /// let winner = replica2.get_winner("myobject").unwrap();
    /// assert_eq!("1-e8e7db1ed2e2e9b7360c9216b8f21353e37ec0365c3d95c51a1302759da9e196", winner);
    /// let block_id = committed_anchors.first().unwrap();
    /// let block2 = replica2.get_block(&block_id).unwrap().unwrap();
    /// let block = replica.get_block(&block_id).unwrap().unwrap();
    /// assert_eq!(block_id, &block.id);
    //// assert_eq!(block_id, &block2.id);
    pub fn meld(&self, other: &Melda) -> Result<Vec<String>> {
        let mut result = vec![];
        let other_data = other.data.read().unwrap();
        let other_items = other_data.list_raw_items("")?;
        if !other_items.is_empty() {
            let mut data = self.data.write().expect("cannot_acquire_data_for_writing");
            let this_items = data.list_raw_items("")?;
            let this_items: HashSet<String> = this_items.into_iter().collect();
            for i in &other_items {
                if !this_items.contains(i) {
                    data.write_raw_item(i, other_data.read_raw_item(i, 0, 0)?.as_slice())?;
                    result.push(i.clone());
                }
            }
        }
        Ok(result)
    }

    /// Reads the data structure and unflattens to a JSON object
    ///
    /// # Example
    /// ```
    /// use melda::{melda::Melda, adapter::Adapter, memoryadapter::MemoryAdapter};
    /// use std::sync::{Arc, Mutex, RwLock};
    /// use serde_json::{Map, Value,json,to_string};
    /// let adapter : Box<dyn Adapter> = Box::new(MemoryAdapter::new());
    /// let adapter = Arc::new(RwLock::new(adapter));
    /// let mut replica = Melda::new(adapter.clone()).expect("cannot_initialize_crdt");
    /// let object = json!({ "somekey" : [ "somedata", 1u32, 2u32, 3u32, 4u32 ] }).as_object().unwrap().clone();
    /// replica.update(object.clone());
    /// let readback = replica.read().unwrap();
    /// assert!(readback.contains_key("somekey"));
    /// let content = serde_json::to_string(&readback).unwrap();
    /// assert_eq!("{\"_id\":\"\u{221A}\",\"somekey\":[\"somedata\",1,2,3,4]}", content);
    /// let adapter : Box<dyn Adapter> = Box::new(MemoryAdapter::new());
    /// let adapter = Arc::new(RwLock::new(adapter));
    /// let mut replica = Melda::new(adapter.clone()).expect("cannot_initialize_crdt");
    /// let object = json!({ "somekey\u{266D}" : [ { "_id": "1", "key" : "alpha" }, { "_id": "2", "key" : "beta" } ] }).as_object().unwrap().clone();
    /// replica.update(object.clone());
    /// let readback = replica.read().unwrap();
    /// assert!(!readback.contains_key("somekey"));
    /// assert!(readback.contains_key("somekey\u{266D}"));
    /// let content = serde_json::to_string(&readback).unwrap();
    /// assert_eq!("{\"_id\":\"\u{221A}\",\"somekey\u{266D}\":[{\"_id\":\"1\",\"key\":\"alpha\"},{\"_id\":\"2\",\"key\":\"beta\"}]}", content);
    /// let info = json!({ "author" : "Some user", "date" : "2022-05-23 13:47:00CET" }).as_object().unwrap().clone();
    /// replica.commit(Some(info));
    /// let adapter2 : Box<dyn Adapter> = Box::new(MemoryAdapter::new());
    /// let adapter2 = Arc::new(RwLock::new(adapter2));
    /// let mut replica2 = Melda::new(adapter2.clone()).expect("cannot_initialize_crdt");
    /// replica2.meld(&replica);
    /// replica2.refresh();
    /// // Continue editing on replica, removing one item
    /// let object = json!({ "somekey\u{266D}" : [ { "_id": "2", "key" : "beta" } ] }).as_object().unwrap().clone();
    /// replica.update(object.clone());
    /// let readback = replica.read().unwrap();
    /// let content = serde_json::to_string(&readback).unwrap();
    /// assert_eq!("{\"_id\":\"\u{221A}\",\"somekey\u{266D}\":[{\"_id\":\"2\",\"key\":\"beta\"}]}", content);
    /// // Commit changes on replica
    /// let info = json!({ "author" : "Some user", "date" : "2022-05-23 13:47:00CET" }).as_object().unwrap().clone();
    /// replica.commit(Some(info));
    /// let readback = replica.read().unwrap();
    /// let content = serde_json::to_string(&readback).unwrap();
    /// assert_eq!("{\"_id\":\"\u{221A}\",\"somekey\u{266D}\":[{\"_id\":\"2\",\"key\":\"beta\"}]}", content);
    /// // Perform some changes on replica2 too
    /// let object = json!({ "somekey\u{266D}" : [ { "_id": "1", "key" : "alpha" }, { "_id": "2", "key" : "beta" }, { "_id": "3", "key" : "gamma" } ] }).as_object().unwrap().clone();
    /// replica2.update(object.clone());
    /// let readback = replica2.read().unwrap();
    /// let content = serde_json::to_string(&readback).unwrap();
    /// assert_eq!("{\"_id\":\"\u{221A}\",\"somekey\u{266D}\":[{\"_id\":\"1\",\"key\":\"alpha\"},{\"_id\":\"2\",\"key\":\"beta\"},{\"_id\":\"3\",\"key\":\"gamma\"}]}", content);
    /// // Commit changes on replica2
    /// let info = json!({ "author" : "Another user", "date" : "2022-05-23 13:48:00CET" }).as_object().unwrap().clone();
    /// replica2.commit(Some(info));
    /// let readback = replica2.read().unwrap();
    /// let content = serde_json::to_string(&readback).unwrap();
    /// assert_eq!("{\"_id\":\"\u{221A}\",\"somekey\u{266D}\":[{\"_id\":\"1\",\"key\":\"alpha\"},{\"_id\":\"2\",\"key\":\"beta\"},{\"_id\":\"3\",\"key\":\"gamma\"}]}", content);
    /// // Meld changes from replica2 back on replica
    /// replica.meld(&replica2);
    /// // Melding does not change the state of replica
    /// let readback = replica.read().unwrap();
    /// let content = serde_json::to_string(&readback).unwrap();
    /// assert_eq!("{\"_id\":\"\u{221A}\",\"somekey\u{266D}\":[{\"_id\":\"2\",\"key\":\"beta\"}]}", content);
    /// // Refresh the state of replica
    /// replica.refresh();
    /// let readback = replica.read().unwrap();
    /// let content = serde_json::to_string(&readback).unwrap();
    /// assert_eq!("{\"_id\":\"\u{221A}\",\"somekey\u{266D}\":[{\"_id\":\"2\",\"key\":\"beta\"},{\"_id\":\"3\",\"key\":\"gamma\"}]}", content);
    pub fn read(&self) -> Result<Map<String, Value>> {
        if !self
            .documents
            .read()
            .expect("failed_to_acquire_documents_for_reading")
            .contains_key(ROOT_ID)
        {
            bail!("no_root")
        } else {
            let c = Mutex::new(HashMap::<String, Map<String, Value>>::new());
            let docs_r = self
                .documents
                .read()
                .expect("failed_to_acquire_documents_for_reading");
            docs_r.par_iter().for_each(|(uuid, rt)| {
                let rt_r = rt
                    .lock()
                    .expect("failed_to_acquire_revision_tree_for_reading");
                if let Some(winner) = rt_r.get_winner() {
                    if !winner.is_deleted() {
                        let mut obj = self.read_object_at_revision(uuid, &rt_r, winner).unwrap();
                        drop(rt_r);
                        obj.insert(ID_FIELD.to_string(), Value::from(uuid.clone()));
                        let mut c_w = c.lock().unwrap();
                        c_w.insert(uuid.to_string(), obj);
                        drop(c_w);
                    }
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
    pub fn update(&self, obj: Map<String, Value>) -> Result<()> {
        let mut extracted_objects = HashMap::<String, Map<String, Value>>::new();
        let path = Vec::<String>::new();
        let root = Value::from(obj);
        // Flatten the structure
        let root = flatten(&mut extracted_objects, &root, &path);
        let root = root.as_str().expect("root_identifier_not_a_string");
        if root != ROOT_ID {
            bail!("invalid_root_id");
        }
        // Check for objects that have disappeared
        // i.e. objects that are found in the current state but are not within the extracted objects
        let docs_r = self
            .documents
            .read()
            .expect("failed_to_acquire_documents_for_reading");
        docs_r
            .par_iter()
            .filter(|(uuid, _)| !extracted_objects.contains_key(*uuid))
            .for_each(|(uuid, _)| {
                self.delete_object(uuid).expect("unable_to_delete_object");
            });
        drop(docs_r);
        // Check for newly created and updated objects
        extracted_objects.into_par_iter().for_each(|(uuid, obj)| {
            //for (uuid, obj) in extracted_objects {
            self.update_object(&uuid, obj)
                .expect("unable_to_update_object");
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
    /// let anchors2 = replica2.commit(None).unwrap().unwrap();
    /// replica2.meld(&replica);
    /// replica2.refresh();
    /// let conflicting = replica2.in_conflict();
    /// assert!(conflicting.contains("myobject"));
    pub fn in_conflict(&self) -> BTreeSet<String> {
        self.documents
            .read()
            .expect("failed_to_acquire_documents_for_reading")
            .par_iter()
            .filter(|(_, rt)| {
                let rt_r = rt
                    .lock()
                    .expect("failed_to_acquire_revision_tree_for_reading");
                let l = rt_r.get_leafs();
                l.len() > 1
            })
            .map(|(uuid, _)| uuid.clone())
            .collect()
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
    /// let value = replica.get_value("myobject", Some(&winner));
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
        match self
            .documents
            .read()
            .expect("cannot_acquire_documents_for_reading")
            .get(uuid.as_ref())
        {
            Some(rt) => {
                let rt_r = rt.lock().expect("cannot_acquire_revision_tree_for_reading");
                match rt_r.get_winner() {
                    Some(r) => Ok(r.to_string()),
                    None => Err(anyhow!("no_winner")),
                }
            }
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
    /// let anchors = replica.commit(None).unwrap().unwrap();
    /// let adapter2 : Box<dyn Adapter> = Box::new(MemoryAdapter::new());
    /// let adapter2 = Arc::new(RwLock::new(adapter2));
    /// let mut replica2 = Melda::new(adapter2.clone()).expect("cannot_initialize_crdt");
    /// let object = json!({ "another" : [ "somedata", 1u32, 2u32, 3u32, 4u32 ] }).as_object().unwrap().clone();
    /// replica2.create_object("myobject", object);
    /// let winner2 = replica2.get_winner("myobject").unwrap();
    /// let anchors2 = replica2.commit(None).unwrap().unwrap();
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
        match self
            .documents
            .read()
            .expect("failed_to_acquire_documents_for_reading")
            .get(uuid.as_ref())
        {
            Some(rt) => {
                let rt_r = rt
                    .lock()
                    .expect("failed_to_acquire_revision_tree_for_reading");
                let w = rt_r.get_winner().ok_or_else(|| anyhow!("no_winner"))?;
                let l = rt_r.get_leafs();
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
    /// let anchors2 = replica2.commit(None).unwrap().unwrap();
    /// replica2.meld(&replica);
    /// replica2.refresh();
    /// let conflicting = replica2.in_conflict();
    /// assert!(conflicting.contains("myobject"));
    /// let revs = replica2.get_conflicting("myobject").unwrap();
    /// assert!(revs.contains(&winner2));
    /// let winner3 = replica2.get_winner("myobject").unwrap();
    /// assert_eq!("1-e8e7db1ed2e2e9b7360c9216b8f21353e37ec0365c3d95c51a1302759da9e196", winner3);    
    /// replica2.resolve_as("myobject", &winner2);
    /// let winner = replica2.get_winner("myobject").unwrap();
    /// assert_eq!("2-255cc6219e48f526c04bc5af86439c34e4fe39fcdc611758ff833a2ff80583f0_e5d1d20", winner);
    /// assert!(replica2.in_conflict().is_empty());
    pub fn resolve_as(&self, uuid: &str, winner: &str) -> Result<String> {
        {
            let winner = Revision::from(winner).expect("invalid_revision_string");
            let docs_r = self
                .documents
                .read()
                .expect("failed_to_acquire_documents_for_reading");
            let rt = docs_r
                .get(uuid)
                .ok_or_else(|| anyhow!("unknown_document"))?;
            let rt_r = rt
                .lock()
                .expect("failed_to_acquire_revision_tree_for_reading");
            {
                let leafs = rt_r.get_leafs();
                // We can only resolve to a status revision
                if !leafs.contains(&winner) {
                    bail!("invalid_winner_revision");
                }
                // If there is only one leaf nothing needs to be resolved
                if leafs.len() <= 1 {
                    bail!("not_in_conflict");
                }
            }
            // Update the winner to ensure that we do not change the view
            let data_r = self.data.read().expect("cannot_acquire_data_for_reading");
            //let merged = data_r.read_object(&winner)?;
            let merged = self.read_object_at_revision(uuid, &rt_r, &winner)?;
            drop(winner);
            drop(rt_r);
            drop(data_r);
            drop(docs_r);
            self.update_object(uuid, merged)?;
        }
        let docs_r = self
            .documents
            .read()
            .expect("failed_to_acquire_documents_for_reading");
        let rt = docs_r
            .get(uuid)
            .ok_or_else(|| anyhow!("unknown_document"))?;
        let rt_r = rt
            .lock()
            .expect("failed_to_acquire_revision_tree_for_reading");
        let winner = rt_r
            .get_winner()
            .expect("revision_tree_invalid_state")
            .clone();
        drop(rt_r);
        drop(docs_r);
        // Seal all other revisions as resolved
        let mut docs_w = self
            .documents
            .write()
            .expect("failed_to_acquire_documents_for_writing");
        let rt = docs_w
            .get_mut(uuid)
            .ok_or_else(|| anyhow!("unknown_document"))?;
        let rt_r = rt
            .get_mut()
            .expect("failed_to_acquire_revision_tree_for_reading");
        let leafs: Vec<Revision> = rt_r.get_leafs().iter().map(|r| (*r).clone()).collect();
        for r in leafs {
            if r != winner {
                let resolved = Revision::new_resolved(&r);
                let rt_w = rt
                    .get_mut()
                    .expect("failed_to_acquire_revision_tree_for_writing");
                rt_w.add(resolved.clone(), Some(r.clone()), true);
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
    /// let value = replica.get_value("myobject", Some(&winner));
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
        let data = self.data.read().expect("cannot_acquire_data_for_reading");
        if data.has_staging() {
            let data_stage = data.stage()?;
            r.insert(OBJECTS_FIELD.to_string(), data_stage);
        }
        if self.has_staging() {
            let mut changes = Vec::<Value>::new();
            for (uuid, rt) in self.documents.read().unwrap().iter() {
                let rt_rw = rt.lock().expect("cannot_acquire_revision_tree_for_commit");
                if rt_rw.has_staging() {
                    rt_rw.get_revisions().iter().for_each(|(rev, rte)| {
                        if rte.is_staging() {
                            if rte.get_parent().is_none() {
                                // Creation record
                                let tuple = vec![uuid.clone(), rev.digest().clone()];
                                changes.push(Value::from(tuple));
                            } else {
                                // Update record
                                let triple = vec![
                                    uuid.clone(),
                                    rte.get_parent().as_ref().unwrap().to_string(),
                                    rev.digest().clone(),
                                ];
                                changes.push(Value::from(triple));
                            }
                        }
                    });
                }
            }
            r.insert(CHANGESETS_FIELD.to_string(), Value::from(changes));
        }
        if !r.is_empty() {
            Ok(Some(Value::from(r)))
        } else {
            Ok(None)
        }
    }

    /// Returns true if there are staged changes
    ///
    /// # Example
    /// ```
    /// use melda::{melda::Melda, adapter::Adapter, memoryadapter::MemoryAdapter};
    /// use std::sync::{Arc, Mutex, RwLock};
    /// use serde_json::{Map, Value,json};
    /// let adapter : Box<dyn Adapter> = Box::new(MemoryAdapter::new());
    /// let adapter = Arc::new(RwLock::new(adapter));
    /// let mut replica = Melda::new(adapter.clone()).expect("cannot_initialize_crdt");
    /// assert!(!replica.has_staging());
    /// let object = json!({ "somekey" : [ "somedata", 1u32, 2u32, 3u32, 4u32 ] }).as_object().unwrap().clone();
    /// let object2 = json!({ "somekey2" : [ "somedata", 1u32, 2u32, 3u32, 4u32 ] }).as_object().unwrap().clone();
    /// replica.create_object("myobject", object.clone());  
    /// assert!(replica.has_staging());
    /// replica.unstage();
    /// assert!(!replica.has_staging());
    /// replica.create_object("myobject", object.clone());  
    /// assert!(replica.has_staging());
    /// let block_id = replica.commit(None).unwrap().unwrap();
    /// assert!(!replica.has_staging());
    /// replica.update_object("myobject", object2.clone());
    /// assert!(replica.has_staging());
    /// let block_id2 = replica.commit(None).unwrap().unwrap();
    /// assert!(!replica.has_staging());
    /// replica.delete_object("myobject");
    /// assert!(replica.has_staging());
    /// ```
    pub fn has_staging(&self) -> bool {
        self.documents
            .read()
            .unwrap()
            .par_iter()
            .any(|(_, rte)| rte.lock().unwrap().has_staging())
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
    /// let value = replica.get_value("myobject", Some(&winner));
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
    pub fn replay_stage(&self, s: &Option<Value>) -> Result<()> {
        if let Some(s) = s {
            if s.is_object() {
                let s = s.as_object().unwrap();
                if s.contains_key(OBJECTS_FIELD) {
                    let o = s.get(OBJECTS_FIELD).unwrap();
                    let mut data = self.data.write().expect("cannot_acquire_data_for_writing");
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
                                        if !self
                                            .documents
                                            .read()
                                            .expect("failed_to_acquire_documents_for_reading")
                                            .contains_key(uuid)
                                        {
                                            let mut rt = RevisionTree::new();
                                            rt.add(r.clone(), None, true);
                                            self.documents
                                                .write()
                                                .unwrap()
                                                .insert(uuid.to_string(), Mutex::new(rt));
                                        } else {
                                            let mut docs = self
                                                .documents
                                                .write()
                                                .expect("failed_to_acquire_documents_for_writing");
                                            let rt = docs.get_mut(uuid).unwrap();
                                            let rt_w = rt.get_mut().expect(
                                                "failed_to_acquire_revision_tree_for_writing",
                                            );
                                            rt_w.add(r.clone(), None, true);
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
                                            prev.index() + 1,
                                            digest.to_string(),
                                            Some(&prev),
                                        );
                                        if !self
                                            .documents
                                            .read()
                                            .expect("failed_to_acquire_documents_for_reading")
                                            .contains_key(uuid)
                                        {
                                            // FIXME: Should this be allowed?
                                            // This might happen if we save the stage, then reload to a previous block
                                            // were an object did not yet exist and then try to re-apply the stage
                                            let mut rt = RevisionTree::new();
                                            rt.add(r, Some(prev), true);
                                            self.documents
                                                .write()
                                                .unwrap()
                                                .insert(uuid.to_string(), Mutex::new(rt));
                                        } else {
                                            let mut docs = self
                                                .documents
                                                .write()
                                                .expect("failed_to_acquire_documents_for_writing");
                                            let rt = docs.get_mut(uuid).unwrap();
                                            let rt_w = rt.get_mut().expect(
                                                "failed_to_acquire_revision_tree_for_writing",
                                            );
                                            rt_w.add(r.clone(), Some(prev.clone()), true);
                                        }
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
    /// let committed_anchors = replica.commit(None).unwrap().unwrap();
    /// let block_id = committed_anchors.first().unwrap();
    /// let block = replica.get_block(&block_id).unwrap().unwrap();
    /// assert_eq!(block_id, &block.id);
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
        let docs = self
            .documents
            .read()
            .expect("failed_to_acquire_documents_for_reading");
        let rt = docs.get(uuid).ok_or_else(|| anyhow!("unknown_document"))?;
        let revision = Revision::from(revision).expect("invalid_revision_string");
        let rt_r = rt
            .lock()
            .expect("failed_to_acquire_revision_tree_for_reading");
        match rt_r.get_parent(&revision) {
            Some(r) => Ok(Some(r.to_string())),
            None => Ok(None),
        }
    }

    // **********************************************************************
    // **********************************************************************
    //
    // DELTA BLOCK SUPPORT FUNCTIONS
    //
    // **********************************************************************
    // **********************************************************************

    // Fetch a block and verify digest
    fn fetch_raw_block(&self, blockid: &str) -> Result<Map<String, Value>> {
        let object = blockid.to_string() + DELTA_EXTENSION;
        let data = self.data.read().expect("cannot_acquire_data_for_reading");
        let data = data.read_raw_item(object.as_str(), 0, 0)?;
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
                        let data = self.data.read().expect("cannot_acquire_data_for_reading");
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
                                let r = Revision::new(
                                    prev.index() + 1,
                                    digest.to_string(),
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
        let data = self.data.read().expect("cannot_acquire_data_for_reading");
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
                let mut docs_w = self
                    .documents
                    .write()
                    .expect("cannot_acquire_documents_for_writing");
                let rt_w = docs_w
                    .entry(uuid.to_string())
                    .or_insert_with(|| Mutex::new(RevisionTree::new()))
                    .get_mut()
                    .expect("cannot_acquire_revision_tree_for_writing");
                rt_w.add(r.clone(), prev.clone(), false);
            }
        };
        Ok(())
    }

    // **********************************************************************
    // **********************************************************************
    //
    // ARRAY DESCRIPTOR SUPPORT FUNCTIONS
    //
    // **********************************************************************
    // **********************************************************************

    // Creates a delta array descriptor from the current obj
    // Returns None if the delta is empty (i.e. the arrays are the same)
    fn create_delta_array_descriptor(
        &self,
        obj: Map<String, Value>,
        rt: &RevisionTree,
    ) -> Result<Option<Map<String, Value>>> {
        let new_descriptor = ArrayDescriptor::new_from_object(obj).expect("malformed_descriptor");
        let winning_order = self
            .rebuild_array_order(rt.get_winner().expect("no_winner"), rt)
            .expect("expecting_winning_order");
        let new_order = new_descriptor.get_order().as_ref().unwrap();
        let patch = make_diff_patch(&winning_order, new_order).expect("failed_diffing");
        if patch.is_empty() {
            Ok(None)
        } else {
            Ok(Some(
                ArrayDescriptor::new_from_patch(patch).to_json_object(),
            ))
        }
    }

    fn read_array_descriptor(&self, revision: &Revision) -> Result<ArrayDescriptor> {
        let data_r = self.data.read().expect("cannot_acquire_data_for_reading");
        let base_object = data_r
            .read_object(revision)
            .expect("cannot_read_base_array_descriptor");
        drop(data_r);
        ArrayDescriptor::new_from_object(base_object)
    }

    // Rebuilds the order by applying all delta patches
    fn rebuild_array_order(
        &self,
        base_revision: &Revision,
        rt: &RevisionTree,
    ) -> Result<Vec<Value>> {
        let mut cache = self.array_descriptors_cache.lock().unwrap();
        if let Some(descriptor) = cache.get(base_revision) {
            Ok(descriptor.get_order().as_ref().unwrap().clone())
        } else {
            let base_descriptor = self.read_array_descriptor(base_revision)?;
            if base_descriptor.is_diff() {
                // We need to resolve the diff, first determine the history
                let mut history = Vec::with_capacity(base_revision.index() as usize);
                let mut current = base_revision;
                while let Some(new_current) = rt.get_parent(current) {
                    history.push(new_current);
                    current = new_current;
                    if cache.contains(current) {
                        break; // Break at last cached descriptor
                    }
                }
                // We have the history of parent revisions, recover the objects
                let mut descriptors = vec![base_descriptor];
                let mut order = vec![];
                descriptors.reserve(history.len());
                for revision in history {
                    if let Some(descriptor) = cache.get(revision) {
                        order = descriptor.get_order().clone().unwrap();
                        break; // Break at last cached descriptor
                    }
                    let descriptor = self.read_array_descriptor(revision)?;
                    if descriptor.is_diff() {
                        descriptors.push(descriptor);
                    } else {
                        order = descriptor.get_order().clone().unwrap();
                        break;
                    }
                }
                // Apply diffs
                for d in descriptors.iter().rev() {
                    let patch = d.get_patch().as_ref().unwrap();
                    apply_diff_patch(&mut order, patch)?;
                }
                cache.put(
                    base_revision.clone(),
                    ArrayDescriptor::new_from_order(order.clone()),
                ); // Only cache the full object
                Ok(order)
            } else {
                Ok(base_descriptor.get_order().clone().unwrap())
            }
        }
    }

    // Get a merged order for the given array descriptor tree
    fn get_merged_order_at_revision(
        &self,
        rt: &RevisionTree,
        base_revision: &Revision,
    ) -> Result<Vec<Value>> {
        // The base object corresponds to the revision we want to keep (winner)
        let leafs = rt.get_leafs();
        if leafs.len() > 1 {
            let mut base_order = self.rebuild_array_order(base_revision, rt)?;
            for l in leafs {
                let leaf_order = self.rebuild_array_order(l, rt)?;
                merge_arrays(&leaf_order, &mut base_order);
            }
            Ok(base_order)
        } else {
            self.rebuild_array_order(base_revision, rt)
        }
    }
}
