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
// along with this program.  If not,ls see <http://www.gnu.org/licenses/>.
use crate::revision::Revision;
use std::{cell::{Cell}, collections::BTreeSet, iter::FromIterator};
use impl_tools::autoimpl;

#[autoimpl(PartialEq, Eq, PartialOrd, Ord ignore self.staging)]
#[autoimpl(Debug, Clone)]
pub struct RevisionTreeEntry {
    revision : Revision,
    parent : Option<Revision>,
    staging : Cell<bool>
}

impl RevisionTreeEntry {
    pub fn new(revision : Revision, parent : Option<Revision>, staging : bool) -> RevisionTreeEntry {
        RevisionTreeEntry {
            revision,
            parent,
            staging : Cell::new(staging)
        }
    }

    pub fn is_staging(&self) -> bool {
        self.staging.get()
    }

    pub fn commit(&self) {
        self.staging.set(false);
    }

    pub fn get_revision(&self) -> &Revision {
        &self.revision
    }

    pub fn get_parent(&self) -> &Option<Revision> {
        &self.parent
    }
}

#[derive(Debug, Clone)]

pub struct RevisionTree {
    revisions: BTreeSet<RevisionTreeEntry>,
    staging : bool,
}

impl RevisionTree {
    /// Constructs a new Revision Tree
    pub fn new() -> RevisionTree {
        RevisionTree {
            revisions: BTreeSet::<RevisionTreeEntry>::new(),
            staging: false,
        }
    }

    /// Add new revision, parent tuple
    /// This method returns true if the pair has been added, false if it already exists
    pub fn add(&mut self, revision: Revision, parent: Option<Revision>, staging: bool) -> bool {
        if self.revisions.insert(RevisionTreeEntry::new(revision, parent, staging)) {
            self.staging |= staging;            
            true
        } else {
            false
        }
    }

    /// Returns the winning revision
    pub fn get_winner(&self) -> Option<&Revision> {
        match self.revisions.iter().max() {
            Some(rte) => Some(&rte.revision),
            None => None,
        }
    }

    /// Returns all revisions
    pub fn get_all_revs(&self) -> BTreeSet<&Revision> {
        FromIterator::from_iter(self.revisions.iter().map(|rte| &rte.revision))
    }

    /// Returns a reference to the internal set
    pub fn get_revisions(&self) -> &BTreeSet<RevisionTreeEntry> {
        &self.revisions
    }

    /// Check if the revision tree is empty
    pub fn is_empty(&self) -> bool {
        self.revisions.is_empty()
    }

    /// Commits staged changes (resets staging flag)
    pub fn commit(&mut self) {
        if self.staging {
            self.revisions.iter().for_each(|rte| {
                rte.commit();
            });
            self.staging = false;
        }
    }

    /// Abort staged changes
    pub fn unstage(&mut self) {
        if self.staging {
            self.revisions.retain(|rte| {
                !rte.staging.get()
            });
            self.staging = false;
        }
    }

    /// Returns wheter there are outstanding changes to commit
    pub fn has_staging(&self) -> bool {
        self.staging
    }

    /// Returns leafs revisions
    pub fn get_leafs(&self) -> BTreeSet<&Revision> {
        let mut leafs = self.get_all_revs();
        self.revisions.iter().for_each(|rte| {
            if rte.revision.is_resolved() {
                leafs.remove(&rte.revision);
            }
            if !rte.parent.is_none() {
                leafs.remove(&rte.parent.as_ref().unwrap());
            }
        });
        leafs
    }

    /// Merges from another Revision Tree
    pub fn merge(&mut self, other: &RevisionTree) {
        self.revisions = self.revisions.union(&other.revisions).cloned().collect();
    }

    /// Returns the parent of a revision
    pub fn get_parent(&self, revision: &Revision) -> Option<&Revision> {
        self.revisions.iter().find_map(|rte| {
            if &rte.revision == revision {
                match &rte.parent {
                    Some(parent) => Some(parent),
                    None => None,
                }
            } else {
                None
            }
        })
    }
}

mod tests {
    #[test]
    fn test_winner() {
        let mut rt = super::RevisionTree::new();
        rt.add(
            crate::revision::Revision::from("3-abc_cde").unwrap(),
            crate::revision::Revision::from("2-abc_cde").ok(),
            true,
        );
        rt.add(
            crate::revision::Revision::from("3-xyz_cde").unwrap(),
            crate::revision::Revision::from("2-abc_cde").ok(),
            true,
        );
        rt.add(
            crate::revision::Revision::from("3-aaa_cde").unwrap(),
            crate::revision::Revision::from("2-abc_cde").ok(),
            true,
        );
        rt.add(
            crate::revision::Revision::from("4-r_cde").unwrap(),
            crate::revision::Revision::from("3-aaa_cde").ok(),
            true,
        );
        rt.add(crate::revision::Revision::from("1-abc").unwrap(), None, true,);
        rt.add(
            crate::revision::Revision::from("2-abc_cde").unwrap(),
            crate::revision::Revision::from("1-abc").ok(),
            true,
        );
        let w = rt.get_winner().unwrap();
        assert!(w.to_string() == "3-xyz_cde");
    }

    #[test]
    fn test_leafs() {
        let mut rt = super::RevisionTree::new();
        rt.add(
            crate::revision::Revision::from("3-abc_cde").unwrap(),
            crate::revision::Revision::from("2-abc_cde").ok(),
            true,
        );
        rt.add(
            crate::revision::Revision::from("3-xyz_cde").unwrap(),
            crate::revision::Revision::from("2-abc_cde").ok(),
            true,
        );
        rt.add(
            crate::revision::Revision::from("3-aaa_cde").unwrap(),
            crate::revision::Revision::from("2-abc_cde").ok(),
            true,
        );
        rt.add(
            crate::revision::Revision::from("4-r_cde").unwrap(),
            crate::revision::Revision::from("3-aaa_cde").ok(),
            true,
        );
        rt.add(
            crate::revision::Revision::from("4-xyz_cde").unwrap(),
            crate::revision::Revision::from("3-xyz_cde").ok(),
            true,
        );
        rt.add(crate::revision::Revision::from("1-abc").unwrap(), None, true,);
        rt.add(
            crate::revision::Revision::from("2-abc_cde").unwrap(),
            crate::revision::Revision::from("1-abc").ok(),
            true,
        );
        let l = rt.get_leafs();
        assert!(l.len() == 2);
        assert!(l.contains(&crate::revision::Revision::from("3-abc_cde").unwrap()));
        assert!(l.contains(&crate::revision::Revision::from("4-xyz_cde").unwrap()));
        // Verify order
        let lvec: Vec<&super::Revision> = l.into_iter().collect();
        assert!(*lvec[0] == crate::revision::Revision::from("3-abc_cde").unwrap());
        assert!(*lvec[1] == crate::revision::Revision::from("4-xyz_cde").unwrap());
        let w = rt.get_winner().unwrap();
        assert!(lvec[1] == w);
    }
}
