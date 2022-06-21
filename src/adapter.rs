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
use anyhow::Result;

/// Initializes an adapter using the provided Url
///
/// # Arguments
///
/// * `url` - An Url for the adapter
/// * `username` - Optional username for authentication (for example, with Solid)
/// * `password` - Optional password for authentication (for example, with Solid)
///
/// # Example
/// ```
/// use melda::{melda::Melda, adapter::get_adapter};
/// use std::sync::{Arc, Mutex, RwLock};
/// use serde_json::{Map, Value,json};
/// let adapter = get_adapter(&url::Url::parse("memory://").unwrap(), None, None).unwrap();
/// let mut replica = Melda::new(Arc::new(RwLock::new(adapter))).expect("cannot_initialize_crdt");
/// ```
pub fn get_adapter(
    url: &reqwest::Url,
    username: Option<String>,
    password: Option<String>,
) -> Result<Box<dyn Adapter>> {
    let mut adapter: Option<Box<dyn Adapter>> = None;

    if url.scheme().eq("memory") {
        adapter = Some(Box::new(crate::memoryadapter::MemoryAdapter::new()));
    } else if url.scheme().eq("file") {
        adapter = Some(Box::new(
            crate::filesystemadapter::FilesystemAdapter::new(url.path())
                .expect("cannot_initialize_adapter"),
        ));
    }
    #[cfg(feature = "solid")]
    if url.scheme().starts_with("solid") {
        adapter = Some(Box::new(
            crate::solidadapter::SolidAdapter::new(
                "https://".to_string() + &url.host().unwrap().to_string(),
                url.path().to_string() + "/",
                username,
                password,
            )
            .expect("cannot_initialize_adapter"),
        ));
    }
    #[cfg(feature = "sqlitedb")]
    if url.scheme().starts_with("sqlite") && !url.path().eq(":memory:") {
        adapter = Some(Box::new(crate::sqliteadapter::SqliteAdapter::new(
            url.path(),
        )));
    } else if url.scheme().starts_with("sqlite") {
        adapter = Some(Box::new(
            crate::sqliteadapter::SqliteAdapter::new_in_memory(),
        ));
    }
    match adapter {
        Some(adapter) => {
            if url.scheme().ends_with("+flate") {
                return Ok(Box::new(crate::flate2adapter::Flate2Adapter::new(
                    std::sync::Arc::new(std::sync::RwLock::new(adapter)),
                )));
            }
            #[cfg(feature = "brotli")]
            if url.scheme().ends_with("+brotli") {
                return Ok(Box::new(crate::brotliadapter::BrotliAdapter::new(
                    std::sync::Arc::new(std::sync::RwLock::new(adapter)),
                )));
            }
            Ok(adapter)
        }
        None => anyhow::bail!("invalid_adapter_url"),
    }
}

/// An adapter implements a storage backend for delta states
pub trait Adapter: Send + Sync {
    /// Reads an object or a sub-object from the backend storage. When offset and length are both 0
    /// the full object is returned, otherwise the sub-object is returned
    ///
    /// # Arguments
    ///
    /// * `key` - The key associated with the object
    /// * `offset` - The starting position of the sub-object in the associated data pack
    /// * `length` - The length of the sub-object (in bytes) in the associated data pack
    ///
    fn read_object(&self, key: &str, offset: usize, length: usize) -> Result<Vec<u8>>;

    /// Writes an object to the storage
    ///
    /// # Arguments
    ///
    /// * `key` - The key associated with the object
    /// * `data` - The content of the object
    fn write_object(&self, key: &str, data: &[u8]) -> Result<()>;

    /// Lists the keys of all objects whose key ends with ext. If ext is an empty string, all objects are returned.
    ///
    /// # Arguments
    ///
    /// * `ext` - The extension (last part of the string) of the requested objects    
    fn list_objects(&self, ext: &str) -> Result<Vec<String>>;
}
