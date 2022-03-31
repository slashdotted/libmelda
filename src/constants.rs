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

///  Suffix for non-reference strings
pub const STRING_ESCAPE_PREFIX: &str = "!";
/// Key suffix to trigger flattening of the associated value
pub const FLATTEN_SUFFIX: &str = "\u{266D}";
/// Patch command to insert a value
pub const PATCH_INSERT: &str = r#"i"#;
/// Patch command to delete a value
pub const PATCH_DELETE: &str = r#"d"#;
/// Data pack extension
pub const PACK_EXTENSION: &str = r#".pack"#;
/// Delta block extension
pub const DELTA_EXTENSION: &str = r#".delta"#;
/// Data pack index extension
pub const INDEX_EXTENSION: &str = r#".index"#;
/// Default root object identifier
pub const ROOT_ID: &str = r#"@"#;
/// Root field key (inside delta blocks)
pub const ROOT_FIELD: &str = r#"r"#;
/// Changesets field key (inside delta blocks and stages)
pub const CHANGESETS_FIELD: &str = r#"c"#;
/// Object map field key (inside stages)
pub const OBJECTS_FIELD: &str = r#"o"#;
/// Full changesets field key (inside delta blocks)
pub const FULL_CHANGESETS_FIELD: &str = r#"C"#;
/// Information field key (inside delta blocks)
pub const INFORMATION_FIELD: &str = r#"i"#;
/// Pack field inside delta blocks
pub const PACK_FIELD: &str = r#"k"#;
/// Hash field (inside objects)
pub const HASH_FIELD: &str = r#"#"#;
/// Expected identifier field (inside objects)
pub const ID_FIELD: &str = r#"_id"#;
/// Hash for empty objects
pub const EMPTY_HASH: &str = r#"e"#;
/// Hash for deleted objects
pub const DELETED_HASH: &str = r#"d"#;
/// Hash for resolved revisions
pub const RESOLVED_HASH: &str = r#"r"#;
/// Key prefix for arrays where deltas are to be computed
pub const DELTA_PREFIX: &str = "\u{0394}";
