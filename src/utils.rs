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
use anyhow::{anyhow, bail, Result};
use serde_json::{json, Map, Value};
use similar::{capture_diff_slices, Algorithm};
use std::collections::HashMap;

const STRING_ESCAPE_PREFIX: &str = "!";
const FLATTEN_SUFFIX: &str = "\u{266D}";

/// Returns true if the key matches a flattened field
pub fn is_flattened_field(key: &str) -> bool {
    key.ends_with(FLATTEN_SUFFIX)
}

/// Escapes a string (add escape prefix)
pub fn escape(s: &str) -> String {
    STRING_ESCAPE_PREFIX.to_string() + s
}

/// Unescapes a string (if necessary)   
pub fn unescape(s: &str) -> String {
    if s.starts_with(STRING_ESCAPE_PREFIX) {
        s[STRING_ESCAPE_PREFIX.len()..].to_string()
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
    // FIXME: Evaluate openssl vs crypto
    //let mut hasher = openssl::sha::Sha256::new();
    //hasher.update(content);
    //hex::encode(hasher.finish())
    let mut hasher = crypto::sha2::Sha256::new();
    crypto::digest::Digest::input(&mut hasher, &content);
    crypto::digest::Digest::result_str(&mut hasher)
}

/// Computes the digest of a JSON object
pub fn digest_object(o: &Map<String, Value>) -> Result<String> {
    if o.is_empty() {
        return Ok("empty".to_string());
    } else if o.contains_key("_id") {
        bail!("identifier_in_object")
    }
    match o.get("#") {
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
pub fn get_identifier(value: &Map<String, Value>, path: &Vec<String>) -> String {
    if value.contains_key("_id") {
        let v = value.get("_id").unwrap();
        if v.is_string() {
            return v.as_str().unwrap().to_owned();
        }
    }
    if path.is_empty() {
        "root".to_string()
    } else {
        digest_string(&path.join(""))
    }
}

/// Merges an array M into another array N
pub fn merge_arrays(order_m: &Vec<Value>, order_n: &mut Vec<Value>) {
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
    let mut current_pos_in_m = 0;
    for t in order_m {
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
        current_pos_in_m += 1;
    }
}

/// Flattens a JSON value, stores promoted objects in c
pub fn flatten(
    c: &mut HashMap<String, Map<String, Value>>,
    value: &Value,
    path: &Vec<String>,
) -> Value {
    match value {
        Value::String(s) => Value::from(escape(s)),
        Value::Array(a) => Value::from(a.iter().map(|v| flatten(c, v, path)).collect::<Vec<_>>()),
        Value::Object(o) => {
            let uuid = get_identifier(o, path);
            let mut fpath = path.clone();
            fpath.push(uuid.clone());
            let no: Map<String, Value> = o
                .into_iter()
                .filter(|(k, _)| *k != "_id")
                .map(|(k, v)| {
                    if k.ends_with(FLATTEN_SUFFIX) {
                        let mut fpath = fpath.clone();
                        fpath.push(k.clone());
                        (k.clone(), flatten(c, v, &fpath))
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
pub fn unflatten(c: &HashMap<String, Map<String, Value>>, value: &Value) -> Option<Value> {
    match value {
        Value::String(s) => {
            if s.starts_with(STRING_ESCAPE_PREFIX) {
                Some(Value::from(unescape(s)))
            } else {
                match c.get(s) {
                    Some(v) => unflatten(c, &Value::from(v.clone())),
                    None => None,
                }
            }
        }
        Value::Array(a) => Some(Value::from(
            a.iter()
                .map(|v| unflatten(c, v))
                .filter(|x| x.is_some())
                .map(|v| v.unwrap())
                .collect::<Vec<_>>(),
        )),
        Value::Object(o) => Some(Value::from(
            o.iter()
                .map(|(k, v)| {
                    if !k.ends_with(FLATTEN_SUFFIX) {
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
pub fn make_diff_patch(old: &Vec<Value>, new: &Vec<Value>) -> Result<Vec<Value>> {
    let olds: Vec<String> = old
        .into_iter()
        .map(|v| serde_json::to_string(v).unwrap())
        .collect();
    let news: Vec<String> = new
        .iter()
        .map(|v| serde_json::to_string(v).unwrap())
        .collect();
    let ops = capture_diff_slices(Algorithm::Myers, &olds, &news);
    let mut patch = vec![];
    for o in ops {
        match o {
            similar::DiffOp::Delete {
                old_index: _,
                old_len,
                new_index,
            } => {
                patch.push(json!(["d", old_len, new_index]));
            }
            similar::DiffOp::Insert {
                old_index: _,
                new_index,
                new_len,
            } => {
                let insertion = &new[new_index..new_index + new_len];
                assert!(insertion.len() == new_len);
                patch.push(json!(["i", new_index, Vec::from(insertion)]));
            }
            similar::DiffOp::Replace {
                old_index: _,
                old_len,
                new_index,
                new_len,
            } => {
                let insertion = &new[new_index..new_index + new_len];
                assert!(insertion.len() == new_len);
                patch.push(json!(["d", old_len, new_index]));
                patch.push(json!(["i", new_index, Vec::from(insertion)]));
                // FIXME: For compatibility encode replace with delete and insert
                // We could replace the two above lines with
                // patch.push(json!(["r", new_index, old_len, Vec::from(insertion)]));
            }
            _ => {}
        }
    }
    Ok(patch)
}

/// Applies a patch to the given array
pub fn apply_diff_patch(old: &mut Vec<Value>, patch: &Vec<Value>) -> Result<()> {
    for op in patch {
        let operation = op[0]
            .as_str()
            .ok_or(anyhow!("invalid_patch_op_not_a_string: {:?}", patch))?;
        if operation == "d" {
            let length = op[1]
                .as_u64()
                .ok_or(anyhow!("invalid_patch_length_not_a_number"))?
                as usize;
            let index = op[2]
                .as_u64()
                .ok_or(anyhow!("invalid_patch_index_not_a_number"))?
                as usize;
            old.drain(index..index + length);
        } else if operation == "i" {
            let index = op[1]
                .as_u64()
                .ok_or(anyhow!("invalid_patch_index_not_a_number"))?
                as usize;
            let items = op[2]
                .as_array()
                .ok_or(anyhow!("invalid_patch_items_not_an_array"))?
                .clone();
            old.splice(index..index, items.into_iter());
        } else if operation == "r" {
            let index = op[1]
                .as_u64()
                .ok_or(anyhow!("invalid_patch_index_not_a_number"))?
                as usize;
            let length = op[2]
                .as_u64()
                .ok_or(anyhow!("invalid_patch_length_not_a_number"))?
                as usize;
            let items = op[3]
                .as_array()
                .ok_or(anyhow!("invalid_patch_items_not_an_array"))?
                .clone();
            old.drain(index..index + length);
            old.splice(index..index, items.into_iter());
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
        assert!(unescape("!") == "");
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
        assert!(digest_object(json!({}).as_object().unwrap()).unwrap() == "empty");
    }

    #[test]
    fn test_get_identifier() {
        let path = vec![];
        assert!(
            get_identifier(
                json!({"_id":"foo","alpha":1234}).as_object_mut().unwrap(),
                &path
            ) == "foo"
        );
        let path: Vec<String> = vec!["foo", "bar", "baz"]
            .into_iter()
            .map(|x| x.to_string())
            .collect();
        assert!(
            get_identifier(json!({"alpha":1234}).as_object_mut().unwrap(), &path)
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
    }

    #[test]
    fn test_flatten() {
        let mut c = HashMap::<String, Map<String, Value>>::new();
        let v = json!({"_id" : "root", "data" : [{"_id": "foo", "value": 3.14}, {"_id": "bar"}]});
        let path = vec![];
        let f = flatten(&mut c, &v, &path);
        assert!(f.is_string());
        assert!(f.as_str().unwrap() == "root");
        assert!(c.len() == 3);
        assert!(c.contains_key("root"));
        assert!(c.contains_key("foo"));
        assert!(c.contains_key("bar"));
        let content = serde_json::to_string(&c.get("root")).unwrap();
        assert!(content == r#"{"data":["foo","bar"]}"#);
        let content = serde_json::to_string(&c.get("foo")).unwrap();
        assert!(content == r#"{"value":3.14}"#);
        let content = serde_json::to_string(&c.get("bar")).unwrap();
        assert!(content == r#"{}"#);
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
