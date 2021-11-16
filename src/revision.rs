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
use anyhow::{bail, Result};
use lazy_static::lazy_static;
use regex::Regex;

use crate::utils::digest_string;

lazy_static! {
    static ref FULL_DELTA_REV: Regex =
        Regex::new(r"(?P<index>\d+)-\u{0394}(?P<digest>\w+)~(?P<delta_digest>\w+)_(?P<tail>\w+)")
            .unwrap();
    static ref FULL_REV: Regex =
        Regex::new(r"(?P<index>\d+)-(?P<digest>\w+)_(?P<tail>\w+)").unwrap();
    static ref FIRST_REV: Regex = Regex::new(r"(?P<index>\d+)-(?P<digest>\w+)").unwrap();
}

#[derive(Debug, Clone)]
pub struct Revision {
    pub index: u32,
    pub digest: String,
    pub delta_digest: Option<String>,
    pub tail: Option<String>,
}

impl Revision {
    /// Returns the null revision
    pub fn null() -> Revision {
        Revision {
            index: 0 as u32,
            digest: String::new(),
            delta_digest: None,
            tail: None,
        }
    }

    /// Constructs a new revision
    pub fn new<T>(index: u32, digest: T, parent: Option<&Revision>) -> Revision
    where
        T: Into<String>,
    {
        Revision {
            index,
            digest: digest.into(),
            delta_digest: None,
            tail: match parent {
            Some(p) => {
                let fulltail = digest_string(&p.to_string());
                Some(fulltail[..7].to_string())
            },
            None => {
                None
            },
            }
        }
    }

    /// Constructs a new revision for a delta object
    pub fn new_with_delta<T>(index: u32, digest: T, delta_digest: T, parent: Option<&Revision>) -> Revision
    where
        T: Into<String>,
    {
        Revision {
            index,
            digest: digest.into(),
            delta_digest: Some(delta_digest.into()),
            tail: match parent {
            Some(p) => {
                let fulltail = digest_string(&p.to_string());
                Some(fulltail[..7].to_string())
            },
            None => {
                None
            },
            }
        }
    }

    /// Constructs a new deleted revision
    pub fn new_deleted(parent: &Revision) -> Revision {
        Revision::new(parent.index + 1, "deleted".to_string(), Some(parent))
    }

    /// Constructs a new empty revision
    #[allow(dead_code)]
    pub fn new_empty(parent: &Revision) -> Revision {
        Revision::new(parent.index + 1, "empty".to_string(), Some(parent))
    }

    /// Constructs a new resolved revision
    #[allow(dead_code)]
    pub fn new_resolved(parent: &Revision) -> Revision {
        Revision::new(parent.index + 1, "resolved".to_string(), Some(parent))
    }

    /// Constructs a new revision from a string
    #[allow(dead_code)]
    pub fn from(s: &str) -> Result<Revision> {
        match FULL_DELTA_REV.captures(s) {
            Some(r) => Ok(Revision {
                index: r.name("index").unwrap().as_str().parse::<u32>().unwrap(),
                digest: r.name("digest").unwrap().as_str().to_string(),
                delta_digest: Some(r.name("delta_digest").unwrap().as_str().to_string()),
                tail: Some(r.name("tail").unwrap().as_str().to_string()),
            }),
            None => match FULL_REV.captures(s) {
                Some(r) => Ok(Revision {
                    index: r.name("index").unwrap().as_str().parse::<u32>().unwrap(),
                    digest: r.name("digest").unwrap().as_str().to_string(),
                    delta_digest: None,
                    tail: Some(r.name("tail").unwrap().as_str().to_string()),
                }),
                None => match FIRST_REV.captures(s) {
                    Some(r) => Ok(Revision {
                        index: r.name("index").unwrap().as_str().parse::<u32>().unwrap(),
                        digest: r.name("digest").unwrap().as_str().to_string(),
                        delta_digest: None,
                        tail: None,
                    }),
                    None => bail!("invalid_revision_string: {}", s),
                },
            },
        }
    }

    /// Returns true if the revision represents a deleted object
    pub fn is_deleted(&self) -> bool {
        self.digest == r#"deleted"#
    }

    /// Returns true if the revision represents a resolved object
    pub fn is_resolved(&self) -> bool {
        self.digest == r#"resolved"#
    }

    /// Returns true if the revision represents an empty object
    pub fn is_empty(&self) -> bool {
        self.digest == r#"empty"#
    }

    pub fn is_delta(&self) -> bool {
        self.delta_digest.is_some()
    }
}

/// Conversion to a string
impl ToString for Revision {
    fn to_string(&self) -> String {
        if self.index > 1 {
            if self.is_delta() {
                self.index.to_string()
                    + &String::from("-\u{0394}")
                    + &self.digest
                    + &String::from("~")
                    + self.delta_digest.as_ref().unwrap()
                    + &String::from("_")
                    + if let Some(t) = &self.tail { t } else { "" }
            } else {
                self.index.to_string()
                    + &String::from("-")
                    + &self.digest
                    + &String::from("_")
                    + if let Some(t) = &self.tail { t } else { "" }
            }
        } else {
            self.index.to_string() + "-" + &self.digest
        }
    }
}

/// Comparison
impl PartialEq for Revision {
    fn eq(&self, other: &Self) -> bool {
        if self.index != other.index {
            false
        } else if self.digest != other.digest {
            false
        } else if self.delta_digest != other.delta_digest {
            false
        } else {
            self.tail.eq(&other.tail)
        }
    }
}

/// Partial Ordering
impl PartialOrd for Revision {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        if self.index < other.index {
            Some(std::cmp::Ordering::Less)
        } else if self.index > other.index {
            Some(std::cmp::Ordering::Greater)
        } else {
            self.to_string().partial_cmp(&other.to_string())
        }
    }
}

/// Equality
impl Eq for Revision {
    fn assert_receiver_is_total_eq(&self) {}
}

/// Full Ordering
impl Ord for Revision {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        if self.is_resolved() {
            std::cmp::Ordering::Less
        } else if self.index < other.index {
            std::cmp::Ordering::Less
        } else if self.index > other.index {
            std::cmp::Ordering::Greater
        } else {
            self.to_string().cmp(&other.to_string())
        }
    }
}

#[cfg(test)]
mod tests {
    #[test]
    fn test_ordering() {
        let r1 = crate::revision::Revision::from("1-alpha_beta").unwrap();
        let r2 = crate::revision::Revision::from("2-alpha_beta").unwrap();
        assert!(r1 == r1);
        assert!(r2 == r2);
        assert!(r1 != r2);
        assert!(r1 < r2);
        assert!(r2 > r1);
    }
}
