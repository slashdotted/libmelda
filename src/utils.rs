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
use anyhow::{anyhow, bail, Result};
use serde_json::{json, Map, Value};
use std::collections::HashMap;
use yavomrs::yavom::{myers_unfilled, Move, Point};

use crate::constants::{
    ARRAY_DESCRIPTOR_ORDER_FIELD, ARRAY_DESCRIPTOR_PREFIX, EMPTY_HASH, FLATTEN_SUFFIX, HASH_FIELD,
    ID_FIELD, PATCH_DELETE, PATCH_INSERT, ROOT_ID, STRING_ESCAPE_PREFIX,
};

/// Returns true if the key matches a flattened field
pub fn is_flattened_field(key: &str) -> bool {
    key.ends_with(FLATTEN_SUFFIX)
}

/// Returns true if the key represents an array descriptor
pub fn is_array_descriptor(key: &str) -> bool {
    key.starts_with(ARRAY_DESCRIPTOR_PREFIX)
}

/// Escapes a string (add escape prefix)
pub fn escape(s: &str) -> String {
    STRING_ESCAPE_PREFIX.to_string() + s
}

/// Unescapes a string (if necessary)   
pub fn unescape(s: &str) -> String {
    if let Some(stripped) = s.strip_prefix(STRING_ESCAPE_PREFIX) {
        stripped.to_string()
    } else {
        s.to_string()
    }
}

/// Computes the digest of a string
pub fn digest_string(content: &str) -> String {
    digest_bytes(content.as_bytes())
}

/// Computes the digest of a slice of bytes
pub fn digest_bytes(content: &[u8]) -> String {
    let mut hasher = openssl::sha::Sha256::new();
    hasher.update(content);
    hex::encode(hasher.finish())
}

/// Computes the digest of a JSON object
pub fn digest_object(o: &Map<String, Value>) -> Result<String> {
    if o.is_empty() {
        return Ok(EMPTY_HASH.to_string());
    } else if o.contains_key(ID_FIELD) {
        bail!("identifier_in_object")
    }
    match o.get(HASH_FIELD) {
        Some(v) => {
            if v.is_string() {
                Ok(v.as_str().unwrap().to_owned())
            } else if v.is_i64() {
                Ok(v.as_i64().unwrap().to_string())
            } else if v.is_f64() {
                Ok(v.as_f64().unwrap().to_string())
            } else {
                bail!("invalid_hash_value_type")
            }
        }
        None => {
            let content = serde_json::to_string(o).unwrap();
            Ok(digest_string(&content))
        }
    }
}

/// Returns the identifier of an object with path
pub fn generate_identifier(value: &Map<String, Value>, path: &[String]) -> Result<String> {
    if value.contains_key(ID_FIELD) {
        let v = value.get(ID_FIELD).unwrap();
        if let Some(v) = v.as_str() {
            if is_array_descriptor(v) {
                Err(anyhow!(
                    "user_object_identifier_cannot_begin_with_array_descriptor_prefix"
                ))
            } else {
                Ok(v.to_owned())
            }
        } else {
            Err(anyhow!("invalid_user_object_identifier"))
        }
    } else if path.is_empty() {
        Ok(ROOT_ID.to_string())
    } else {
        // We assume that the string digest does not start with
        // the ARRAY_DESCRIPTOR_PREFIX (which is true,
        // since the prefix is the ^ character by default
        // and the digest is an hex string)
        Ok(digest_string(&path.join("")))
    }
}

/// Merges an array M into another array N
pub fn merge_arrays(order_m: &[Value], order_n: &mut Vec<Value>) {
    if order_n.is_empty() {
        order_m.iter().for_each(|t| order_n.push(t.clone()));
        return;
    }
    if order_m.is_empty() {
        return;
    }
    // Find the pivot
    let mut ins_pos_in_n = 0;
    let mut pivot_pos_in_m: usize = 0;
    for t in order_m {
        let index = order_n.iter().position(|e| *e == *t);
        match index {
            Some(position) => {
                ins_pos_in_n = position;
                break;
            }
            None => pivot_pos_in_m += 1,
        }
    }
    for (current_pos_in_m, t) in order_m.iter().enumerate() {
        // Search t in N
        let it = order_n.iter().position(|e| *e == *t);
        match it {
            // If found, update the insertion point to this new position
            Some(position) => ins_pos_in_n = position,
            None => {
                // Is the current position (in M) before the pivot
                if current_pos_in_m < pivot_pos_in_m {
                    // Insert at insertIt
                    order_n.insert(ins_pos_in_n, t.clone());
                    pivot_pos_in_m = current_pos_in_m
                } else {
                    ins_pos_in_n += 1;
                    order_n.insert(ins_pos_in_n, t.clone());
                }
            }
        }
    }
}

/// Flattens a JSON value, stores promoted objects in c
pub fn flatten(
    c: &mut HashMap<String, Map<String, Value>>,
    value: &Value,
    path: &[String],
) -> Value {
    match value {
        Value::String(s) => Value::from(escape(s)),
        Value::Array(a) => Value::from(a.iter().map(|v| flatten(c, v, path)).collect::<Vec<_>>()),
        Value::Object(o) => {
            let uuid = generate_identifier(o, path).unwrap();
            let mut fpath = path.to_owned();
            fpath.push(uuid.clone());
            let no: Map<String, Value> = o
                .into_iter()
                .filter(|(k, _)| *k != ID_FIELD)
                .map(|(k, v)| {
                    if is_flattened_field(k) {
                        let mut fpath = fpath.clone();
                        fpath.push(k.clone());
                        let flattened = flatten(c, v, &fpath);
                        if let Value::Array(_) = &flattened {
                            // We assume that all arrays will be stored as deltas from
                            // the previous version
                            let mut array_descriptor_object = Map::new();
                            array_descriptor_object
                                .insert(ARRAY_DESCRIPTOR_ORDER_FIELD.to_string(), flattened);
                            let array_descriptor_uuid = ARRAY_DESCRIPTOR_PREFIX.to_string()
                                + &digest_string(&fpath.join(""));
                            c.insert(array_descriptor_uuid.clone(), array_descriptor_object);
                            (k.clone(), Value::from(array_descriptor_uuid))
                        } else {
                            (k.clone(), flattened)
                        }
                    } else {
                        (k.clone(), v.clone())
                    }
                })
                .collect();
            c.insert(uuid.clone(), no);
            Value::from(uuid)
        }
        _ => value.clone(),
    }
}

/// Unflattens a collection of objects starting from an initial value
pub fn unflatten(c: &mut HashMap<String, Map<String, Value>>, value: &Value) -> Option<Value> {
    match value {
        Value::String(s) => {
            if s.starts_with(STRING_ESCAPE_PREFIX) {
                Some(Value::from(unescape(s)))
            } else if is_array_descriptor(s) {
                // Fetch corresponding descriptor (and remove it from the collection, since we will not use it multiple times)
                let v = c.remove(s);
                match v {
                    Some(v) => {
                        if let Some(v) = v.get(ARRAY_DESCRIPTOR_ORDER_FIELD) {
                            if let Some(order) = v.as_array() {
                                let mut array: Vec<Value> = vec![];
                                for uuid in order {
                                    if let Some(uuid) = uuid.as_str() {
                                        // We remove the object from the collection when we use it
                                        if let Some(o) = c.remove(uuid) {
                                            if let Some(item) =
                                                unflatten(c, &Value::from(o))
                                            {
                                                array.push(item);
                                            }
                                        }
                                    }
                                }
                                Some(Value::from(array))
                            } else {
                                panic!("expecting_order_field_in_descriptor_as_array")
                            }
                        } else {
                            panic!("expecting_order_field_in_descriptor")
                        }
                    }
                    None => panic!("unknown_descriptor_object"),
                }
            } else {
                match c.remove(s) {
                    Some(v) => unflatten(c, &Value::from(v)),
                    None => Some(json!(null)),
                }
            }
        }
        Value::Array(a) => Some(Value::from(
            a.iter().filter_map(|v| unflatten(c, v)).collect::<Vec<_>>(),
        )),
        Value::Object(o) => Some(Value::from(
            o.iter()
                .map(|(k, v)| {
                    if !is_flattened_field(k) {
                        (k.clone(), v.clone())
                    } else {
                        (k.clone(), unflatten(c, v).unwrap())
                    }
                })
                .collect::<Map<String, Value>>(),
        )),
        _ => Some(value.clone()),
    }
}

/// Creates an array diff patch
pub fn make_diff_patch(old: &[Value], new: &[Value]) -> Result<Vec<Value>> {
    let ops = myers_unfilled(old, new);
    let mut patch = vec![];
    for o in ops {
        let Move(op, s, t, _) = o;
        match op {
            yavomrs::yavom::OP::INSERT => {
                let count = t.1 - s.1;
                let from = s.1 as usize;
                let to = (s.1 + count) as usize;
                patch.push(json!([PATCH_INSERT, s.1, &new[from..to]]));
            }
            yavomrs::yavom::OP::DELETE => {
                let count = t.0 - s.0;
                patch.push(json!([PATCH_DELETE, count, s.1]));
            }
            yavomrs::yavom::OP::_DELETE => {
                let Point(count, start) = s;
                patch.push(json!([PATCH_DELETE, count, start]));
            }
        }
    }
    Ok(patch)
}

/// Applies a patch to the given array
pub fn apply_diff_patch(old: &mut Vec<Value>, patch: &[Value]) -> Result<()> {
    for op in patch {
        let operation = op[0]
            .as_str()
            .ok_or_else(|| anyhow!("invalid_patch_op_not_a_string: {:?}", patch))?;
        if operation == PATCH_DELETE {
            let length = op[1]
                .as_u64()
                .ok_or_else(|| anyhow!("invalid_patch_length_not_a_number"))?
                as usize;
            let index = op[2]
                .as_u64()
                .ok_or_else(|| anyhow!("invalid_patch_index_not_a_number"))?
                as usize;
            old.drain(index..index + length);
        } else if operation == PATCH_INSERT {
            let index = op[1]
                .as_u64()
                .ok_or_else(|| anyhow!("invalid_patch_index_not_a_number"))?
                as usize;
            let items = op[2]
                .as_array()
                .ok_or_else(|| anyhow!("invalid_patch_items_not_an_array"))?
                .clone();
            old.splice(index..index, items.into_iter());
        } else {
            return Err(anyhow!("invalid_patch_op"));
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    macro_rules! string_value_vec {
        ($($str:expr),*) => ({
            vec![$(Value::from($str),)*] as Vec<Value>
        });
    }

    fn vec_equals<T: PartialEq>(a: &Vec<T>, b: &Vec<T>) -> bool {
        let matching = a.iter().zip(b.iter()).filter(|&(a, b)| a == b).count();
        matching == a.len() && matching == b.len()
    }

    #[test]
    fn test_escape() {
        assert!(escape("hello world") == "!hello world");
        assert!(escape("") == "!");
        assert!(escape("!") == "!!");
    }

    #[test]
    fn test_unescape() {
        assert!(unescape("!hello world") == "hello world");
        assert!(unescape("!").is_empty());
        assert!(unescape("!!") == "!");
    }

    #[test]
    fn test_digest_string() {
        assert!(
            digest_string("hello world")
                == "b94d27b9934d3e08a52e52d7da7dabfac484efe37a5380ee9088f7ace2efcde9"
        );
    }

    #[test]
    fn test_digest_bytes() {
        assert!(
            digest_bytes("hello world".as_bytes())
                == "b94d27b9934d3e08a52e52d7da7dabfac484efe37a5380ee9088f7ace2efcde9"
        );
    }

    #[test]
    fn test_digest_object() {
        assert!(
            digest_object(json!({"alpha":1234}).as_object().unwrap()).unwrap()
                == "54564897e73b8babc49d21c5c062987c1edd5bda9bba99ae3e4c810d0cb3afc0"
        );
        assert!(
            digest_object(json!({"alpha":1234}).as_object().unwrap()).unwrap()
                == "54564897e73b8babc49d21c5c062987c1edd5bda9bba99ae3e4c810d0cb3afc0"
        );
        assert!(
            digest_object(json!({"alpha": 1234}).as_object().unwrap()).unwrap()
                == "54564897e73b8babc49d21c5c062987c1edd5bda9bba99ae3e4c810d0cb3afc0"
        );
        assert!(digest_object(json!({}).as_object().unwrap()).unwrap() == EMPTY_HASH);
    }

    #[test]
    fn test_get_identifier() {
        let path = vec![];
        assert!(
            generate_identifier(
                json!({"_id":"foo","alpha":1234}).as_object_mut().unwrap(),
                &path
            )
            .unwrap()
                == "foo"
        );
        let path: Vec<String> = vec!["foo", "bar", "baz"]
            .into_iter()
            .map(|x| x.to_string())
            .collect();
        assert!(
            generate_identifier(json!({"alpha":1234}).as_object_mut().unwrap(), &path).unwrap()
                == digest_string("foobarbaz")
        );
    }

    #[test]
    fn test_merge_arrays() {
        {
            let mut a = string_value_vec!["A"];
            let mut b = string_value_vec!["A", "B", "C", "D", "E", "F"];
            merge_arrays(&a, &mut b);
            merge_arrays(&b, &mut a);
            assert!(vec_equals(&a, &b));
        }
        {
            let mut a = string_value_vec![];
            let mut b = string_value_vec!["A", "C", "D", "E", "F"];
            merge_arrays(&a, &mut b);
            merge_arrays(&b, &mut a);
            assert!(vec_equals(&a, &b));
        }
        {
            let mut a = string_value_vec!["A", "B", "C"];
            let mut b = string_value_vec![];
            merge_arrays(&a, &mut b);
            merge_arrays(&b, &mut a);
            assert!(vec_equals(&a, &b));
        }
        {
            let mut a = string_value_vec!["A", "B", "C", "F", "G"];
            let mut b = string_value_vec!["A"];
            merge_arrays(&a, &mut b);
            merge_arrays(&b, &mut a);
            assert!(vec_equals(&a, &b));
        }
        {
            let mut a = string_value_vec!["A", "B", "C", "F", "G"];
            let mut b = string_value_vec!["A", "C", "D", "E", "F"];
            merge_arrays(&a, &mut b);
            merge_arrays(&b, &mut a);
            assert!(vec_equals(&a, &b));
        }
        {
            let mut a = string_value_vec!["A", "C", "B", "G"];
            let mut b = string_value_vec!["F", "D", "E", "A"];
            merge_arrays(&a, &mut b);
            merge_arrays(&b, &mut a);
            assert!(vec_equals(&a, &b));
        }
        {
            let mut a = string_value_vec!["A", "B"];
            let mut b = string_value_vec!["F", "D"];
            merge_arrays(&a, &mut b);
            merge_arrays(&b, &mut a);
            assert!(vec_equals(&a, &b));
        }
        {
            let mut a = string_value_vec!["B", "A"];
            let mut b = string_value_vec!["F", "D"];
            merge_arrays(&a, &mut b);
            merge_arrays(&b, &mut a);
            assert!(vec_equals(&a, &b));
        }
        {
            let mut a = string_value_vec!["B", "A"];
            let mut b = string_value_vec!["F", "A", "D"];
            merge_arrays(&a, &mut b);
            merge_arrays(&b, &mut a);
            assert!(vec_equals(&a, &b));
        }
        {
            let mut a = string_value_vec!["A", "F", "C", "B", "G"];
            let mut b = string_value_vec!["F", "C", "D", "E", "A"];
            merge_arrays(&a, &mut b);
            merge_arrays(&b, &mut a);
            assert!(!vec_equals(&a, &b));
        }
        {
            let mut a = json!(["B", 1, 5, 9, "A"]).as_array().unwrap().clone();
            let mut b = json!(["F", 7, "A", "D"]).as_array().unwrap().clone();
            merge_arrays(&a, &mut b);
            merge_arrays(&b, &mut a);
            assert!(vec_equals(&a, &b));
        }
        {
            let mut a = string_value_vec!["A", "B", "Y", "D", "E"];
            let mut b = string_value_vec!["A", "B", "X", "D", "E"];
            merge_arrays(&a, &mut b);
            merge_arrays(&b, &mut a);
            assert!(vec_equals(&a, &b));
        }
    }

    #[test]
    fn test_flatten() {
        {
            let mut c = HashMap::<String, Map<String, Value>>::new();
            let v = json!({ID_FIELD: ROOT_ID, "data" : [{ID_FIELD: "foo", "value": 1.23}, {ID_FIELD: "bar"}]});
            let path = vec![];
            let f = flatten(&mut c, &v, &path);
            assert!(f.is_string());
            assert!(f.as_str().unwrap() == ROOT_ID);
            assert!(c.len() == 1);
        }
        {
            let mut c = HashMap::<String, Map<String, Value>>::new();
            let v = json!({ID_FIELD : ROOT_ID, "data\u{266D}" : [{ID_FIELD: "foo", "value": 1.23}, {ID_FIELD: "bar"}]});
            let path = vec![];
            let f = flatten(&mut c, &v, &path);
            assert!(f.is_string());
            assert!(f.as_str().unwrap() == ROOT_ID);
            assert!(c.len() == 4);
            assert!(c.contains_key(ROOT_ID));
            assert!(c.contains_key("foo"));
            assert!(c.contains_key("bar"));
            let content = serde_json::to_string(&c.get(ROOT_ID)).unwrap();
            assert!(
                content
                    == r#"{"data♭":"^e13aaf01b21510d633e7e19d055f67c73f93a417d9b5a0099f76513f86dc6b00"}"#
            );
            let content = serde_json::to_string(&c.get("foo")).unwrap();
            assert!(content == r#"{"value":1.23}"#);
            let content = serde_json::to_string(&c.get("bar")).unwrap();
            assert!(content == r#"{}"#);
        }
    }

    #[test]
    fn test_unflatten() {
        {
            let mut c = HashMap::<String, Map<String, Value>>::new();
            let v = json!({ID_FIELD: ROOT_ID, "data" : [{ID_FIELD: "foo", "value": 1.23}, {ID_FIELD: "bar"}]});
            let path = vec![];
            let f = flatten(&mut c, &v, &path);
            assert!(f.is_string());
            assert!(f.as_str().unwrap() == ROOT_ID);
            assert!(c.len() == 1);
            // Create map of objects with ids
            let mut mc = HashMap::<String, Map<String, Value>>::new();
            c.iter().for_each(|(k, v)| {
                let mut vn = v.clone();
                vn.insert(ID_FIELD.to_string(), serde_json::Value::String(k.clone()));
                mc.insert(k.clone(), vn);
            });
            let rootobj = mc.get(ROOT_ID).unwrap().clone();
            let obj = unflatten(&mut mc, &serde_json::Value::from(rootobj)).unwrap();
            let reconstructed = serde_json::to_string(&obj).unwrap();
            let original = serde_json::to_string(&v).unwrap();
            assert!(reconstructed == original);
        }
        {
            let mut c = HashMap::<String, Map<String, Value>>::new();
            let v = json!({ID_FIELD : ROOT_ID, "data\u{266D}" : [{ID_FIELD: "foo", "value": 1.23}, {ID_FIELD: "bar"}]});
            let path = vec![];
            let f = flatten(&mut c, &v, &path);
            assert!(f.is_string());
            assert!(f.as_str().unwrap() == ROOT_ID);
            assert!(c.len() == 4);
            assert!(c.contains_key(ROOT_ID));
            assert!(c.contains_key("foo"));
            assert!(c.contains_key("bar"));
            let content = serde_json::to_string(&c.get(ROOT_ID)).unwrap();
            assert!(
                content
                    == r#"{"data♭":"^e13aaf01b21510d633e7e19d055f67c73f93a417d9b5a0099f76513f86dc6b00"}"#
            );
            let content = serde_json::to_string(&c.get("foo")).unwrap();
            assert!(content == r#"{"value":1.23}"#);
            let content = serde_json::to_string(&c.get("bar")).unwrap();
            assert!(content == r#"{}"#);
            // Create map of objects with ids
            let mut mc = HashMap::<String, Map<String, Value>>::new();
            c.iter().for_each(|(k, v)| {
                let mut vn = v.clone();
                vn.insert(ID_FIELD.to_string(), serde_json::Value::String(k.clone()));
                mc.insert(k.clone(), vn);
            });
            let rootobj = mc.get(ROOT_ID).unwrap().clone();
            let obj = unflatten(&mut mc, &serde_json::Value::from(rootobj)).unwrap();
            let reconstructed = serde_json::to_string(&obj).unwrap();
            let original = serde_json::to_string(&v).unwrap();
            assert!(reconstructed == original);
        }
    }

    #[test]
    fn test_patch() {
        {
            let mut a = string_value_vec!["A", "B", "C", "D", "E"];
            let b = string_value_vec!["X", "A", "B", "D"];
            let patch = make_diff_patch(&a, &b).unwrap();
            apply_diff_patch(&mut a, &patch).unwrap();
            assert!(b == a);
        }
        {
            let mut a = string_value_vec!["A", "B", "C", "D", "E"];
            let b = string_value_vec!["X", "Y", "Z", "D", "E"];
            let patch = make_diff_patch(&a, &b).unwrap();
            apply_diff_patch(&mut a, &patch).unwrap();
            assert!(b == a);
        }
        {
            let mut a = string_value_vec!["A", "B", "C", "D", "E"];
            let b = string_value_vec!["X", "Y", "Z", "D"];
            let patch = make_diff_patch(&a, &b).unwrap();
            apply_diff_patch(&mut a, &patch).unwrap();
            assert!(b == a);
        }
        {
            let mut a = string_value_vec!["A", "B", "C"];
            let b = string_value_vec!["X", "Y", "Z", "D", "E"];
            let patch = make_diff_patch(&a, &b).unwrap();
            apply_diff_patch(&mut a, &patch).unwrap();
            assert!(b == a);
        }
    }
}
