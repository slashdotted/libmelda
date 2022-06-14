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
