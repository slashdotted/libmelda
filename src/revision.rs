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
use anyhow::{bail, Result};
use lazy_static::lazy_static;
use regex::Regex;
use std::fmt;
use std::hash::Hash;

use crate::constants::{DELETED_HASH, EMPTY_HASH, RESOLVED_HASH};
use crate::utils::digest_string;

lazy_static! {
    static ref FULL_REV: Regex =
        Regex::new(r"(?P<index>\d+)-(?P<digest>\w+)_(?P<tail>\w+)").unwrap();
    static ref FIRST_REV: Regex = Regex::new(r"(?P<index>\d+)-(?P<digest>\w+)").unwrap();
}

#[derive(Debug, Clone)]
pub struct Revision {
    index: u32,
    digest: String,
    tail: Option<String>,
}

impl Revision {
    /// Returns the null revision
    #[allow(dead_code)]
    pub fn null() -> Revision {
        Revision {
            index: 0_u32,
            digest: String::new(),
            tail: None,
        }
    }

    pub fn digest(&self) -> &String {
        &self.digest
    }

    pub fn index(&self) -> u32 {
        self.index
    }

    pub fn is_charcode(&self) -> bool {
        self.digest.len() <= 8 && u32::from_str_radix(&self.digest, 16).is_ok()
    }

    /// Constructs a new revision
    pub fn new<T>(index: u32, digest: T, parent: Option<&Revision>) -> Revision
    where
        T: Into<String>,
    {
        Revision {
            index,
            digest: digest.into(),
            tail: match parent {
                Some(p) => {
                    let fulltail = digest_string(&p.to_string());
                    Some(fulltail[..7].to_string())
                }
                None => None,
            },
        }
    }

    /// Constructs a new revision
    pub fn new_updated<T>(digest: T, parent: &Revision) -> Revision
    where
        T: Into<String>,
    {
        Revision {
            index: parent.index + 1,
            digest: digest.into(),
            tail: {
                let fulltail = digest_string(&parent.to_string());
                Some(fulltail[..7].to_string())
            },
        }
    }

    /// Constructs a new deleted revision
    pub fn new_deleted(parent: &Revision) -> Revision {
        Revision::new(parent.index + 1, DELETED_HASH.to_string(), Some(parent))
    }

    /// Constructs a new empty revision
    #[allow(dead_code)]
    pub fn new_empty(parent: &Revision) -> Revision {
        Revision::new(parent.index + 1, EMPTY_HASH.to_string(), Some(parent))
    }

    /// Constructs a new resolved revision
    #[allow(dead_code)]
    pub fn new_resolved(parent: &Revision) -> Revision {
        Revision::new(parent.index + 1, RESOLVED_HASH.to_string(), Some(parent))
    }

    /// Constructs a new revision from a string
    #[allow(dead_code)]
    pub fn from(s: &str) -> Result<Revision> {
        match FULL_REV.captures(s) {
            Some(r) => Ok(Revision {
                index: r.name("index").unwrap().as_str().parse::<u32>().unwrap(),
                digest: r.name("digest").unwrap().as_str().to_string(),
                tail: Some(r.name("tail").unwrap().as_str().to_string()),
            }),
            None => match FIRST_REV.captures(s) {
                Some(r) => Ok(Revision {
                    index: r.name("index").unwrap().as_str().parse::<u32>().unwrap(),
                    digest: r.name("digest").unwrap().as_str().to_string(),
                    tail: None,
                }),
                None => bail!("invalid_revision_string: {}", s),
            },
        }
    }

    /// Returns true if the revision represents a deleted object
    pub fn is_deleted(&self) -> bool {
        self.digest == DELETED_HASH
    }

    /// Returns true if the revision represents a resolved object
    pub fn is_resolved(&self) -> bool {
        self.digest == RESOLVED_HASH
    }

    /// Returns true if the revision represents an empty object
    pub fn is_empty(&self) -> bool {
        self.digest == EMPTY_HASH
    }
}

/// Basic hash implementation
impl Hash for Revision {
    fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
        self.index.hash(state);
        self.digest.hash(state);
        self.tail.hash(state);
    }
}

/// Conversion to a string
impl fmt::Display for Revision {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        if self.index > 1 {
            write!(
                f,
                "{}-{}_{}",
                self.index,
                &self.digest,
                if let Some(t) = &self.tail { t } else { "" }
            )
        } else {
            write!(f, "{}-{}", self.index, &self.digest)
        }
    }
}

/// Comparison
impl PartialEq for Revision {
    fn eq(&self, other: &Self) -> bool {
        if self.index != other.index || self.digest != other.digest {
            false
        } else {
            self.tail.eq(&other.tail)
        }
    }
}

/// Partial Ordering
impl PartialOrd for Revision {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(other))
    }
}

/// Equality
impl Eq for Revision {
    fn assert_receiver_is_total_eq(&self) {}
}

/// Full Ordering
impl Ord for Revision {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        if self.is_resolved() && other.is_resolved() {
            self.to_string().cmp(&other.to_string())
        } else if self.is_resolved() {
            // Resolved revisions always have the least priority
            std::cmp::Ordering::Less
        } else if other.is_resolved() {
            std::cmp::Ordering::Greater
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

    #[test]
    fn test_charcode() {
        let r1 = crate::revision::Revision::from("1-alpha_beta").unwrap();
        let r2 = crate::revision::Revision::from("2-1234_beta").unwrap();
        let r3 = crate::revision::Revision::from("2-abcdef12_beta").unwrap();
        let r4 = crate::revision::Revision::from("2-abcdef12abc_beta").unwrap();
        assert!(!r1.is_charcode());
        assert!(r2.is_charcode());
        assert!(r3.is_charcode());
        assert!(!r4.is_charcode());
    }
}
