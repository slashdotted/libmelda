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
use std::{collections::BTreeSet, iter::FromIterator};

#[derive(Debug, Clone)]
pub struct RevisionTree {
    revisions: BTreeSet<(Revision, Option<Revision>)>,
}

impl RevisionTree {
    /// Constructs a new Revision Tree
    pub fn new() -> RevisionTree {
        RevisionTree {
            revisions: BTreeSet::<(Revision, Option<Revision>)>::new(),
        }
    }

    /// Add new revision, parent tuple
    /// This method returns true if the pair has been added, false if it already exists
    pub fn add(&mut self, revision: Revision, parent: Option<Revision>) -> bool {
        self.revisions.insert((revision, parent))
    }

    /// Removes an existing revision, parent tuple
    /// This methods returns true if the pair was present (and then removed), false otherwise
    pub fn remove(&mut self, revision: Revision, parent: Option<Revision>) -> bool {
        self.revisions.remove(&(revision, parent))
    }

    /// Returns the winning revision
    pub fn get_winner(&self) -> Option<&Revision> {
        match self.revisions.iter().max() {
            Some((r, _)) => Some(r),
            None => None,
        }
    }

    /// Returns all revisions
    pub fn get_all_revs(&self) -> BTreeSet<&Revision> {
        FromIterator::from_iter(self.revisions.iter().map(|(revision, _)| revision))
    }

    pub fn get_revisions(&self) -> &BTreeSet<(Revision, Option<Revision>)> {
        &self.revisions
    }

    /// Returns leafs revisions
    pub fn get_leafs(&self) -> BTreeSet<&Revision> {
        let mut leafs = self.get_all_revs();
        self.revisions.iter().for_each(|(rev, parent)| {
            if rev.is_resolved() {
                leafs.remove(rev);
            }
            if !parent.is_none() {
                leafs.remove(&parent.as_ref().unwrap());
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
        self.revisions.iter().find_map(|(rev, parent)| {
            if rev == revision {
                match parent {
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
        );
        rt.add(
            crate::revision::Revision::from("3-xyz_cde").unwrap(),
            crate::revision::Revision::from("2-abc_cde").ok(),
        );
        rt.add(
            crate::revision::Revision::from("3-aaa_cde").unwrap(),
            crate::revision::Revision::from("2-abc_cde").ok(),
        );
        rt.add(
            crate::revision::Revision::from("4-r_cde").unwrap(),
            crate::revision::Revision::from("3-aaa_cde").ok(),
        );
        rt.add(crate::revision::Revision::from("1-abc").unwrap(), None);
        rt.add(
            crate::revision::Revision::from("2-abc_cde").unwrap(),
            crate::revision::Revision::from("1-abc").ok(),
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
        );
        rt.add(
            crate::revision::Revision::from("3-xyz_cde").unwrap(),
            crate::revision::Revision::from("2-abc_cde").ok(),
        );
        rt.add(
            crate::revision::Revision::from("3-aaa_cde").unwrap(),
            crate::revision::Revision::from("2-abc_cde").ok(),
        );
        rt.add(
            crate::revision::Revision::from("4-r_cde").unwrap(),
            crate::revision::Revision::from("3-aaa_cde").ok(),
        );
        rt.add(
            crate::revision::Revision::from("4-xyz_cde").unwrap(),
            crate::revision::Revision::from("3-xyz_cde").ok(),
        );
        rt.add(crate::revision::Revision::from("1-abc").unwrap(), None);
        rt.add(
            crate::revision::Revision::from("2-abc_cde").unwrap(),
            crate::revision::Revision::from("1-abc").ok(),
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
