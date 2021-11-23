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
use anyhow::{bail, Result};
use cacache;
use lru::LruCache;
use oxiri::Iri;
use reqwest::blocking::Client;
use reqwest::header::HeaderMap;
use rio_api::model::NamedNode;
use rio_api::parser::TriplesParser;
use rio_turtle::{TurtleError, TurtleParser};
use std::cell::RefCell;
use std::path::Path;
use std::sync::Mutex;
use std::{collections::HashMap, env};
use url::Url;

pub struct SolidAdapter {
    username: String,
    password: String,
    folder: String,
    url: String,
    client: Client,
    cache: Mutex<RefCell<LruCache<String, String>>>,
    disk_cache_dir: String,
}

pub enum ResourceType {
    File,
    Folder,
}

impl SolidAdapter {
    pub fn new(
        url: String,
        folder: String,
        username: Option<String>,
        password: Option<String>,
    ) -> Result<Self> {
        // On disk cache
        let disk_cache_dir = std::env::temp_dir()
            .join(".solidcache")
            .into_os_string()
            .into_string()
            .unwrap();

        let u = if username.is_some() {
            username.unwrap()
        } else {
            env::var("MELDA_SOLID_USERNAME")?
        };
        let p = if password.is_some() {
            password.unwrap()
        } else {
            env::var("MELDA_SOLID_PASSWORD")?
        };
        let sa = SolidAdapter {
            username: u,
            password: p,
            folder,
            url: url,
            client: Client::builder().cookie_store(true).build()?,
            cache: Mutex::new(RefCell::new(LruCache::<String, String>::new(1024))),
            disk_cache_dir,
        };
        sa.authenticate()?;
        Ok(sa)
    }

    fn authenticate(&self) -> Result<()> {
        let target = self.url.clone() + "/login/password";
        let mut params = HashMap::new();
        params.insert("username", self.username.as_str());
        params.insert("password", self.password.as_str());
        let response = self.client.post(target).form(&params).send()?;
        if response.status() == 200 {
            Ok(())
        } else {
            bail!("cannot_authenticate");
        }
    }

    fn fetch_object(&self, key: &str) -> Result<String> {
        let cache = self.cache.lock().unwrap();
        let mut cache = cache.borrow_mut();
        match cache.get(&key.to_string()) {
            Some(v) => Ok(v.clone()),
            None => {
                // Try to read from disk cache
                match cacache::read_sync(&self.disk_cache_dir, key) {
                    Ok(data) => Ok(String::from_utf8_lossy(&data).into_owned()),
                    Err(_) => {
                        let (_, url) = self.get_object_url(key)?;
                        let response = self.client.get(url).send()?;
                        if response.status().as_u16() == 200 {
                            let data = response.text()?;
                            cache.put(key.to_string(), data.clone());
                            cacache::write_sync(&self.disk_cache_dir, key, data.as_bytes())?;
                            Ok(data)
                        } else {
                            bail!("cannot_read_object")
                        }
                    }
                }
            }
        }
    }

    fn get_object_url(&self, key: &str) -> Result<(String, Url)> {
        let prefix = &key[..2];
        let objecturl = self.url.clone() + self.folder.as_str() + &prefix + "/" + key;
        Ok((prefix.to_string(), Url::parse(&objecturl)?))
    }

    fn ensure_container_exists(&self, key: &str) -> Result<Url> {
        let (prefix, object_url) = self.get_object_url(key)?;
        let base_url = self.url.clone() + self.folder.as_str();
        let response = self.client.head(base_url.clone()).send()?;
        if response.status().as_u16() != 200 {
            let mut headers = HeaderMap::new();
            headers.insert("Content-Type", "text/turtle".parse().unwrap());
            headers.insert(
                "Link",
                "<http://www.w3.org/ns/ldp#BasicContainer>; rel=\"type\""
                    .parse()
                    .unwrap(),
            );
            headers.insert("Slug", prefix.parse().unwrap());
            let response = self.client.post(base_url).headers(headers).send()?;
            if response.status().as_u16() != 201 && response.status().as_u16() != 409 {
                bail!("cannot_ensure_container_exists");
            }
        }
        Ok(object_url)
    }

    fn list_container(
        &self,
        ext: &str,
        target: &String,
        restype: ResourceType,
    ) -> Result<Vec<String>> {
        let mut list = vec![];
        let response = self.client.get(target).send()?;
        let data = response.text()?;
        let rdf_type = NamedNode {
            iri: "http://www.w3.org/1999/02/22-rdf-syntax-ns#type",
        };
        let ldp_resource = NamedNode {
            iri: "http://www.w3.org/ns/ldp#Resource",
        };
        let base_iri = Iri::parse(target.clone()).unwrap();
        TurtleParser::new(data.as_bytes(), Some(base_iri)).parse_all(&mut |t| {
            if t.predicate == rdf_type && t.object == ldp_resource.into() {
                match t.subject {
                    rio_api::model::Subject::NamedNode(nn) => match Url::parse(nn.iri) {
                        Ok(u) => {
                            match restype {
                                ResourceType::File => {
                                    if !u.to_string().ends_with("/") {
                                        // Skip subfolders
                                        let dp = Path::new(u.path());
                                        let fname =
                                            dp.file_name().unwrap().to_str().unwrap().to_string();
                                        if fname.ends_with(ext) {
                                            let fname =
                                                fname.strip_suffix(ext).unwrap().to_string();
                                            list.push(fname);
                                        }
                                    }
                                }
                                ResourceType::Folder => {
                                    if u.to_string().ends_with("/") {
                                        // Skip files
                                        let dp = Path::new(u.path());
                                        let fname =
                                            dp.file_name().unwrap().to_str().unwrap().to_string();
                                        if fname.ends_with(ext) {
                                            let fname =
                                                fname.strip_suffix(ext).unwrap().to_string();
                                            list.push(fname + "/");
                                        }
                                    }
                                }
                            }
                        }
                        Err(_) => (),
                    },
                    _ => (),
                }
            }
            Ok(()) as Result<(), TurtleError>
        })?;
        Ok(list)
    }
}

impl Adapter for SolidAdapter {
    fn read_object(&self, key: &str, offset: usize, length: usize) -> Result<Vec<u8>> {
        let data = self.fetch_object(key)?;
        if offset == 0 && length == 0 {
            Ok(data.as_bytes().to_vec())
        } else {
            Ok(data.as_bytes()[offset..offset + length].to_vec())
        }
    }

    fn write_object(&self, key: &str, data: &[u8]) -> Result<()> {
        let cache = self.cache.lock().unwrap();
        let mut cache = cache.borrow_mut();
        if !cache.contains(&key.to_string()) {
            let url = self.ensure_container_exists(key)?;
            let response = self.client.head(url.clone()).send()?;
            if response.status().as_u16() != 200 {
                let mut headers = HeaderMap::new();
                headers.insert("Content-Type", "application/octet-stream".parse().unwrap());
                let response = self
                    .client
                    .put(url)
                    .headers(headers)
                    .body(data.to_vec())
                    .send()?;
                if response.status().as_u16() >= 200 || response.status().as_u16() <= 204 {
                    cache.put(key.to_string(), String::from_utf8_lossy(&data).into_owned());
                    cacache::write_sync(&self.disk_cache_dir, key, data)?;
                } else {
                    bail!("cannot_write_object");
                }
            }
        }
        Ok(())
    }

    fn list_objects(&self, ext: &str) -> Result<Vec<String>> {
        let mut list = vec![];
        let target = self.url.clone() + self.folder.as_str();
        for sub in self.list_container("", &target, ResourceType::Folder)? {
            let target = self.url.clone() + self.folder.as_str() + &sub;
            let mut partial = self
                .list_container(ext, &target, ResourceType::File)
                .unwrap();
            list.append(&mut partial);
        }
        Ok(list)
    }
}
