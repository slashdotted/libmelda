// Melda - Delta State JSON CRDT
// Copyright (C) 2022 Amos Brocco <amos.brocco@supsi.ch>
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
    fs::{create_dir_all, metadata, read_dir, File},
    io::{Read, Write},
    path::{Path, PathBuf},
};

pub struct Flate2FilesystemAdapter {
    path: PathBuf,
}

impl Flate2FilesystemAdapter {
    pub fn new(dir: &str) -> Result<Flate2FilesystemAdapter, &str> {
        let dp = Path::new(dir);
        if !dp.exists() {
            create_dir_all(dp).expect("failed_to_create_directory");
        }
        if !dp.is_dir() {
            Err("not_a_directory")
        } else {
            Ok(Flate2FilesystemAdapter {
                path: PathBuf::from(dir),
            })
        }
    }

    fn get_object_path(&self, key: &str) -> Result<(String, PathBuf)> {
        let prefix = &key[..2];
        let subdirectory = self.path.clone().join(&prefix).join(key);
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

impl Adapter for Flate2FilesystemAdapter {
    fn read_object(&self, key: &str, offset: usize, length: usize) -> Result<Vec<u8>> {
        let (_, filepath) = self.get_object_path(key)?;
        let mut f = File::open(&filepath)?;
        let metadata = metadata(&filepath)?;
        let mut data = vec![0; metadata.len() as usize];
        f.read_exact(&mut data)?;
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
        let (_, filepath) = self.ensure_container_exists(key)?;
        if !filepath.exists() {
            let mut e = DeflateEncoder::new(Vec::new(), Compression::default());
            e.write_all(data)?;
            let mut f = File::create(filepath)?;
            let compressed = e.finish().unwrap();
            f.write_all(&compressed)?;
            f.flush()?;
        }
        Ok(())
    }

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
