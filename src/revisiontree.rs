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
use std::{cell::Cell, collections::{BTreeMap, BTreeSet}};
use impl_tools::autoimpl;

#[autoimpl(PartialEq, Eq, PartialOrd, Ord ignore self.staging)]
#[autoimpl(Debug, Clone)]
pub struct RevisionTreeEntry {
    parent : Option<Revision>,
    staging : Cell<bool>
}

impl RevisionTreeEntry {
    pub fn new(parent : Option<Revision>, staging : bool) -> RevisionTreeEntry {
        RevisionTreeEntry {
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

    pub fn get_parent(&self) -> &Option<Revision> {
        &self.parent
    }
}

#[derive(Debug, Clone)]

pub struct RevisionTree {
    revisions: BTreeMap<Revision,RevisionTreeEntry>,
    staging : bool,
    leafs: BTreeSet<Revision>, // Revisions that are not parents
    ghost_parents: BTreeSet<Revision>, // Revisions that are parents but are not in revisions
}

impl RevisionTree {
    /// Constructs a new Revision Tree
    pub fn new() -> RevisionTree {
        RevisionTree {
            revisions: BTreeMap::<Revision,RevisionTreeEntry>::new(),
            staging: false,
            leafs: BTreeSet::<Revision>::new(),
            ghost_parents: BTreeSet::<Revision>::new(),
        }
    }

    /// Add new revision, parent tuple
    /// This method returns true if the pair has been added, false if it already exists
    pub fn add(&mut self, revision: Revision, parent: Option<Revision>, staging: bool) -> bool {
        // Insert the new revision information
        if self.revisions.insert(revision.clone(), RevisionTreeEntry::new(parent.clone(), staging)).is_none() {
            // If the revision was a ghost parent it is not anymore
            if !self.ghost_parents.remove(&revision) {
                // The revision is not a parent, therefore it can be considered as a leaf
                self.leafs.insert(revision);
            }
            // Store the parent information
            if let Some(p) = parent {
                // If the parent is an unknown revision, store it as ghost parent
                if !self.revisions.contains_key(&p) {
                    self.ghost_parents.insert(p.clone());
                } else {
                    // Otherwise be sure that it's not considered a leaf
                    self.leafs.remove(&p);
                }
            }        
            self.staging |= staging;            
            true
        } else {
            false
        }
    }

    /// Returns the winning revision
    pub fn get_winner(&self) -> Option<&Revision> {
        match self.revisions.iter().max() {
            Some(rte) => Some(&rte.0),
            None => None,
        }
    }

    /// Returns a reference to the internal set
    pub fn get_revisions(&self) -> &BTreeMap<Revision,RevisionTreeEntry> {
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
                rte.1.commit();
            });
            self.staging = false;
        }
    }

    /// Abort staged changes
    pub fn unstage(&mut self) {
        if self.staging {
            self.revisions.retain(|_, rte| {
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
    pub fn get_leafs(&self) -> &BTreeSet<Revision> {
        &self.leafs
    }

    /// Merges from another Revision Tree
    pub fn merge(&mut self, other: &RevisionTree) {
        let mut source = other.revisions.clone();
        self.revisions.append(&mut source);
    }

    /// Returns the parent of a revision
    pub fn get_parent(&self, revision: &Revision) -> Option<&Revision> {
        self.revisions.iter().find_map(|(rev,rte)| {
            if rev == revision {
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
