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
// along with this program.  If not,ls see <http://www.gnu.org/licenses/>.
use crate::revision::Revision;
use impl_tools::autoimpl;
use std::collections::{BTreeMap, BTreeSet, HashSet};

#[autoimpl(PartialEq, Eq, PartialOrd, Ord ignore self.staging)]
#[autoimpl(Debug, Clone)]
pub struct RevisionTreeEntry {
    parent: Option<Revision>,
    staging: bool,
}

impl RevisionTreeEntry {
    pub fn new(parent: Option<Revision>, staging: bool) -> Self {
        Self { parent, staging }
    }

    pub fn is_staging(&self) -> bool {
        self.staging
    }

    pub fn commit(&mut self) {
        self.staging = false;
    }

    pub fn get_parent(&self) -> &Option<Revision> {
        &self.parent
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ValidationState {
    Validated,
    NonValidated,
}

#[derive(Debug, Clone)]
pub struct RevisionTree {
    revisions: BTreeMap<Revision, RevisionTreeEntry>,
    staging: bool,

    // Cache
    leafs_cache: BTreeSet<Revision>,
    winner_cache: Option<Revision>,

    state: ValidationState,
}

impl RevisionTree {
    pub fn new() -> Self {
        Self {
            revisions: BTreeMap::new(),
            staging: false,
            leafs_cache: BTreeSet::new(),
            winner_cache: None,
            state: ValidationState::Validated,
        }
    }

    pub fn unvalidated_add(
        &mut self,
        revision: Revision,
        parent: Option<Revision>,
        staging: bool,
    ) -> bool {
        if self.revisions.contains_key(&revision) {
            return false;
        }

        self.revisions
            .insert(revision, RevisionTreeEntry::new(parent, staging));

        self.staging |= staging;
        self.state = ValidationState::NonValidated;

        true
    }

    pub fn add(&mut self, revision: Revision, parent: Option<Revision>, staging: bool) -> bool {
        let result = self.unvalidated_add(revision, parent, staging);
        if result {
            self.validate();
        }
        result
    }

    pub fn validate(&mut self) {
        self.leafs_cache.clear();
        self.winner_cache = None;
        let mut parents = HashSet::new();
        for entry in self.revisions.values() {
            if let Some(p) = entry.get_parent() {
                if self.revisions.contains_key(p) {
                    parents.insert(p.clone());
                }
            }
        }
        for r in self.revisions.keys() {
            if parents.contains(r) {
                continue;
            }
            if r.is_resolved() {
                continue;
            }
            if self.is_valid(r) {
                self.leafs_cache.insert(r.clone());
            }
        }
        self.winner_cache = self.leafs_cache.iter().max().cloned();
        self.state = ValidationState::Validated;
    }

    fn is_valid(&self, rev: &Revision) -> bool {
        let mut r = rev;
        loop {
            let entry = match self.revisions.get(r) {
                Some(e) => e,
                None => return false,
            };
            if r.index() == 1 && entry.get_parent().is_none() {
                return true;
            }
            match entry.get_parent() {
                Some(p) => r = p,
                None => return false,
            }
        }
    }

    pub fn get_leafs(&self) -> &BTreeSet<Revision> {
        match self.state {
            ValidationState::Validated => &self.leafs_cache,
            ValidationState::NonValidated => {
                panic!("revisiontree_not_validated")
            }
        }
    }

    pub fn get_winner(&self) -> Option<&Revision> {
        match self.state {
            ValidationState::Validated => self.winner_cache.as_ref(),
            ValidationState::NonValidated => {
                panic!("revisiontree_not_validated")
            }
        }
    }

    pub fn get_revisions(&self) -> &BTreeMap<Revision, RevisionTreeEntry> {
        &self.revisions
    }

    pub fn is_empty(&self) -> bool {
        self.revisions.is_empty()
    }

    pub fn commit(&mut self) {
        match self.state {
            ValidationState::Validated => {
                if self.staging {
                    for entry in self.revisions.values_mut() {
                        entry.commit();
                    }
                    self.staging = false;
                }
            }
            ValidationState::NonValidated => {
                panic!("revisiontree_not_validated")
            }
        }
    }

    pub fn unstage(&mut self) {
        if self.staging {
            self.revisions.retain(|_, entry| !entry.is_staging());
            self.staging = false;
            self.state = ValidationState::NonValidated;
        }
        self.validate();
    }

    pub fn has_staging(&self) -> bool {
        self.staging
    }

    pub fn get_parent(&self, revision: &Revision) -> Option<&Revision> {
        self.revisions.get(revision)?.get_parent().as_ref()
    }
}

mod tests {
    #[test]
    #[should_panic]
    fn test_unvalidated_revision_tree() {
        let mut rt = super::RevisionTree::new();
        rt.add(
            crate::revision::Revision::from("3-abc_cde").unwrap(),
            crate::revision::Revision::from("2-abc_cde").ok(),
            true,
        );
        rt.unvalidated_add(
            crate::revision::Revision::from("3-xyz_cde").unwrap(),
            crate::revision::Revision::from("2-abc_cde").ok(),
            true,
        );
        let _ = rt.get_winner();
    }

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
        rt.add(
            crate::revision::Revision::from("1-abc").unwrap(),
            None,
            true,
        );
        rt.add(
            crate::revision::Revision::from("2-abc_cde").unwrap(),
            crate::revision::Revision::from("1-abc").ok(),
            true,
        );
        rt.validate();
        let w = rt.get_winner().unwrap();
        assert!(w.to_string() == "3-xyz_cde");
    }

    #[test]
    fn test_leafs() {
        {
            let mut rt = super::RevisionTree::new();
            rt.add(
                crate::revision::Revision::from("3-abc_cde").unwrap(),
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
            rt.add(
                crate::revision::Revision::from("1-abc").unwrap(),
                None,
                true,
            );
            rt.add(
                crate::revision::Revision::from("2-abc_cde").unwrap(),
                crate::revision::Revision::from("1-abc").ok(),
                true,
            );
            rt.validate();
            let l = rt.get_leafs();
            assert!(l.len() == 1);
            assert!(l.contains(&crate::revision::Revision::from("3-abc_cde").unwrap()));
            let w = rt.get_winner().unwrap();
            assert_eq!(&crate::revision::Revision::from("3-abc_cde").unwrap(), w);
        }
        {
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
            rt.add(
                crate::revision::Revision::from("1-abc").unwrap(),
                None,
                true,
            );
            rt.add(
                crate::revision::Revision::from("2-abc_cde").unwrap(),
                crate::revision::Revision::from("1-abc").ok(),
                true,
            );
            rt.validate();
            let l = rt.get_leafs();
            assert!(l.len() == 2);
            assert!(l.contains(&crate::revision::Revision::from("3-abc_cde").unwrap()));
            assert!(l.contains(&crate::revision::Revision::from("4-xyz_cde").unwrap()));
            let w = rt.get_winner().unwrap();
            assert_eq!(&crate::revision::Revision::from("4-xyz_cde").unwrap(), w);
        }
    }
}
