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
use anyhow::{bail, Result};
use std::{
    convert::TryInto,
    fs::{create_dir_all, metadata, read_dir, File},
    io::{Read, Seek, SeekFrom, Write},
    path::{Path, PathBuf},
};

/// Implements storage in a folder on the filesystem
pub struct FilesystemAdapter {
    path: PathBuf,
}

impl FilesystemAdapter {
    /// Creates a new adapter to store data in the specified directory
    ///
    /// # Arguments
    ///
    /// * `dir` - The path to the directory where data is to be saved to (if the directory does not exist it will be crated)
    pub fn new(dir: &str) -> Result<FilesystemAdapter, &str> {
        let dp = Path::new(dir);
        if !dp.exists() {
            create_dir_all(dp).expect("failed_to_create_directory");
        }
        if !dp.is_dir() {
            Err("not_a_directory")
        } else {
            Ok(FilesystemAdapter {
                path: PathBuf::from(dir),
            })
        }
    }

    fn get_object_path(&self, key: &str) -> Result<(String, PathBuf)> {
        let prefix = &key[..2];
        let subdirectory = self.path.clone().join(prefix).join(key);
        Ok((prefix.to_string(), subdirectory))
    }

    fn ensure_container_exists(&self, key: &str) -> Result<(String, PathBuf)> {
        let (prefix, target) = self.get_object_path(key)?;
        let path = target
            .as_path()
            .parent()
            .expect("failed_to_get_parent_path");
        if !path.exists() {
            create_dir_all(path)?;
        }
        if !path.is_dir() {
            Err(anyhow::anyhow!("not_a_directory"))
        } else {
            Ok((prefix, target.clone()))
        }
    }
}

impl Adapter for FilesystemAdapter {
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
        let (_, filepath) = self.get_object_path(key)?;
        if length == 0 {
            let mut f = File::open(&filepath)?;
            let metadata = metadata(&filepath)?;
            let mut data = vec![0; metadata.len() as usize];
            f.read_exact(&mut data)?;
            Ok(data)
        } else {
            let mut data = vec![0; length];
            let mut f = File::open(&filepath)?;
            let metadata = metadata(&filepath)?;
            if metadata.len() < (offset + length).try_into().unwrap() {
                bail!("out_of_bounds")
            }
            f.seek(SeekFrom::Start(offset.try_into().unwrap()))?;
            f.read_exact(&mut data)?;
            Ok(data)
        }
    }

    /// Writes an object to the storage
    ///
    /// # Arguments
    ///
    /// * `key` - The key associated with the object
    /// * `data` - The content of the object    
    fn write_object(&self, key: &str, data: &[u8]) -> Result<()> {
        let (_, filepath) = self.ensure_container_exists(key)?;
        if !filepath.exists() {
            let mut f = File::create(filepath)?;
            f.write_all(data)?;
            f.flush()?;
        }
        Ok(())
    }

    /// Lists the keys of all objects whose key ends with ext. If ext is an empty string, all objects are returned.
    ///
    /// # Arguments
    ///
    /// * `ext` - The extension (last part of the string) of the requested objects     
    fn list_objects(&self, ext: &str) -> Result<Vec<String>> {
        let content = read_dir(self.path.clone())?;
        let mut result = vec![];
        for sd in content {
            match sd {
                Ok(de) => {
                    // Recursively list process contents
                    let subcontent = read_dir(de.path())?;
                    for f in subcontent {
                        match f {
                            Ok(subde) => {
                                let dp = subde.path();
                                if dp.is_file() {
                                    let fname =
                                        dp.file_name().unwrap().to_str().unwrap().to_string();
                                    if fname.ends_with(ext) {
                                        let fname = fname.strip_suffix(ext).unwrap().to_string();
                                        result.push(fname);
                                    }
                                }
                            }
                            Err(_) => continue,
                        }
                    }
                }
                Err(_) => continue,
            }
        }
        if result.is_empty() {
            Ok(vec![])
        } else {
            Ok(result)
        }
    }
}

#[cfg(test)]
mod tests {
    use mktemp::Temp;

    use crate::{adapter::Adapter, flate2adapter::Flate2Adapter};

    use super::FilesystemAdapter;

    #[test]
    fn test_filesystem_read_object_flate() {
        let temp = Temp::new_dir().unwrap();
        let path_buf = temp.to_path_buf();
        let sa = FilesystemAdapter::new(path_buf.to_str().unwrap()).unwrap();
        let ma: Box<dyn Adapter> = Box::new(sa);
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
    fn test_filesystem_write_object_flate() {
        let temp = Temp::new_dir().unwrap();
        let path_buf = temp.to_path_buf();
        let sa = FilesystemAdapter::new(path_buf.to_str().unwrap()).unwrap();
        let ma: Box<dyn Adapter> = Box::new(sa);
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
    fn test_filesystem_list_objects_flate() {
        let temp = Temp::new_dir().unwrap();
        let path_buf = temp.to_path_buf();
        let sa = FilesystemAdapter::new(path_buf.to_str().unwrap()).unwrap();
        let ma: Box<dyn Adapter> = Box::new(sa);
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

    #[test]
    fn test_filesystem_read_object() {
        let temp = Temp::new_dir().unwrap();
        let path_buf = temp.to_path_buf();
        let sqa = FilesystemAdapter::new(path_buf.to_str().unwrap()).unwrap();
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
    fn test_filesystem_write_object() {
        let temp = Temp::new_dir().unwrap();
        let path_buf = temp.to_path_buf();
        let sqa = FilesystemAdapter::new(path_buf.to_str().unwrap()).unwrap();
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
    fn test_filesystem_list_objects() {
        let temp = Temp::new_dir().unwrap();
        let path_buf = temp.to_path_buf();
        let sqa = FilesystemAdapter::new(path_buf.to_str().unwrap()).unwrap();
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
