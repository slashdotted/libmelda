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
use fs_extra::dir::get_size;
use melda::flate2filesystemadapter::Flate2FilesystemAdapter;
use melda::{adapter::Adapter, filesystemadapter::FilesystemAdapter, melda::Melda};
use serde_json::{json, Map, Value};
use std::sync::{Arc, RwLock};
use std::{
    fs::File,
    io::{self, BufRead},
    path::Path,
    process::exit,
    time::{SystemTime, UNIX_EPOCH},
};
use uuid::Uuid;

const CHAR_KEY: &str = "\u{0394}c\u{266D}";

fn read_lines<P>(filename: P) -> io::Result<io::Lines<io::BufReader<File>>>
where
    P: AsRef<Path>,
{
    let file = File::open(filename)?;
    Ok(io::BufReader::new(file).lines())
}

fn push_char_single(root_obj: &mut Map<String, Value>, pos: usize, c: &str) -> usize {
    if c.len() != 1 {
        panic!("{} too long", c);
    }
    let mut newlength: usize = 0;
    if let Value::Array(characters) = root_obj.get_mut(CHAR_KEY).unwrap() {
        let mut chardata = Map::<String, Value>::new();
        chardata.insert(
            "#".to_string(),
            Value::from(format!("{:x}", c.as_bytes()[0] as u8)),
        );
        chardata.insert(
            "_id".to_string(),
            Value::from(Uuid::new_v4().to_simple().to_string()),
        );
        characters.insert(pos, Value::from(chardata));
        newlength = characters.len();
    }
    newlength
}

// Note: in the benchmark there is no deletion that affects \n, therefore we can skip joining the lines
fn remove_char_single(root_obj: &mut Map<String, Value>, pos: usize) -> usize {
    let mut newlength: usize = 0;
    if let Value::Array(characters) = root_obj.get_mut(CHAR_KEY).unwrap() {
        characters.remove(pos);
        newlength = characters.len();
    }
    newlength
}

fn main() {
    let page_size = procfs::page_size().unwrap() as usize;
    let args: Vec<String> = std::env::args().collect();
    if args.len() < 2 {
        eprintln!("Missing command: build|buildflate <dir> <interval> [maxdeltas] | buildreload|buildreloadflate <dir> <interval> [maxdeltas] | read|readflate <dir>");
        exit(1);
    }

    let command = &args[1];

    if command == "build"
        || command == "buildbench"
        || command == "buildreload"
        || command == "buildflate"
        || command == "buildreloadflate"
    {
        if args.len() < 3 {
            eprintln!("Missing directory");
            exit(1);
        } else if args.len() < 4 {
            eprintln!("Missing interval");
            exit(1);
        }
        let mut maxdeltas = if args.len() == 5 {
            args[4].parse::<i32>().unwrap()
        } else {
            -1
        };
        let interval = &args[3].parse::<u32>().unwrap();
        let dir = args[2].as_str();
        let benchreload = command == "buildreload" || command == "buildreloadflate";
        let file_adapter: Box<dyn Adapter> = if command == "buildreload" || command == "build" {
            Box::new(FilesystemAdapter::new(&dir).expect("cannot_initialize_adapter"))
        } else {
            Box::new(Flate2FilesystemAdapter::new(&dir).expect("cannot_initialize_adapter"))
        };
        let mut replica =
            Melda::new(Arc::new(RwLock::new(file_adapter))).expect("cannot_initialize_crdt");
        let mut input = vec![];
        let statm = procinfo::pid::statm_self().unwrap();
        println!(
            "Initial memory {} vm, {} resident",
            statm.size * page_size,
            statm.resident * page_size
        );
        if let Ok(lines) = read_lines("./editing-trace.txt") {
            for line in lines {
                if let Ok(content) = line {
                    let json: Vec<Value> = serde_json::from_str(&content).unwrap();
                    input.push(json);
                }
            }
        }
        let statm = procinfo::pid::statm_self().unwrap();
        println!(
            "After load memory {} vm, {} resident",
            statm.size * page_size,
            statm.resident * page_size
        );
        let start = SystemTime::now().duration_since(UNIX_EPOCH).unwrap();
        // Initialize the "empty" document
        let mut base_doc = json!({ CHAR_KEY: Value::from(Vec::<Value>::new()) })
            .as_object()
            .unwrap()
            .clone();

        let mut i = 0; // Iteration
        let mut last_commit_i = 0;
        let mut insertions = 0;
        let mut deletions = 0;
        let mut deltas = 0;
        let mut length = 0;
        // Process the editing trace
        for json in input {
            if json[1].as_u64().unwrap() > 0 {
                // deletion
                let pos = json[0].as_u64().unwrap();
                length = remove_char_single(&mut base_doc, pos as usize);
                deletions += 1;
            } else if json.len() > 2 {
                // insert
                let pos = json[0].as_u64().unwrap();
                let c = json[2].as_str().unwrap();
                length = push_char_single(&mut base_doc, pos as usize, c);
                insertions += 1;
            }

            if i % interval == 0 {
                let elapsed = SystemTime::now().duration_since(UNIX_EPOCH).unwrap() - start;
                let update_start = SystemTime::now().duration_since(UNIX_EPOCH).unwrap();
                replica.update(base_doc.clone()).expect("failed_to_update");
                let update_elapsed =
                    SystemTime::now().duration_since(UNIX_EPOCH).unwrap() - update_start;
                let commit_start = SystemTime::now().duration_since(UNIX_EPOCH).unwrap();
                replica.commit(None, false).expect("failed_to_commit");
                let commit_elapsed =
                    SystemTime::now().duration_since(UNIX_EPOCH).unwrap() - commit_start;
                deltas += 1;
                maxdeltas -= 1;
                let state_size = if command != "buildbench" {
                    get_size(&dir).unwrap()
                } else {
                    0
                };
                let eps = i as f64 / elapsed.as_secs_f64();
                if benchreload {
                    let reload_start = SystemTime::now().duration_since(UNIX_EPOCH).unwrap();
                    replica.reload().unwrap();
                    let reload_elapsed =
                        SystemTime::now().duration_since(UNIX_EPOCH).unwrap() - reload_start;
                    let statm = procinfo::pid::statm_self().unwrap();
                    println!("{},edits,{},ins,{},del,{},real_length,{},array_length,{},deltas,{},ms,{},eps,{},state_size,{},update_ms,{},commit_ms,{},reload_ms,{},statm.size,{},statm.resident,{},statm.share,{},statm.text,{},statm.data", i, insertions, deletions, insertions-deletions, length, deltas, elapsed.as_millis(), eps, state_size, update_elapsed.as_millis(), commit_elapsed.as_millis(), reload_elapsed.as_millis(), statm.size * page_size, statm.resident * page_size, statm.share * page_size, statm.text * page_size, statm.data * page_size);
                } else {
                    let statm = procinfo::pid::statm_self().unwrap();
                    println!("{},edits,{},ins,{},del,{},real_length,{},array_length,{},deltas,{},ms,{},eps,{},state_size,{},update_ms,{},commit_ms,{},reload_ms,{},statm.size,{},statm.resident,{},statm.share,{},statm.text,{},statm.data", i, insertions, deletions, insertions-deletions, length, deltas, elapsed.as_millis(), eps, state_size, update_elapsed.as_millis(), commit_elapsed.as_millis(), -1, statm.size * page_size, statm.resident * page_size, statm.share * page_size, statm.text * page_size, statm.data * page_size);
                }
                if maxdeltas == 0 {
                    exit(0);
                }
                last_commit_i = i;
            }
            i += 1;
        }
        if last_commit_i != i - 1 {
            let elapsed = SystemTime::now().duration_since(UNIX_EPOCH).unwrap() - start;
            let update_start = SystemTime::now().duration_since(UNIX_EPOCH).unwrap();
            replica.update(base_doc.clone()).expect("failed_to_update");
            let update_elapsed =
                SystemTime::now().duration_since(UNIX_EPOCH).unwrap() - update_start;
            let commit_start = SystemTime::now().duration_since(UNIX_EPOCH).unwrap();
            replica.commit(None, false).expect("failed_to_commit");
            let commit_elapsed =
                SystemTime::now().duration_since(UNIX_EPOCH).unwrap() - commit_start;
            deltas += 1;
            let state_size = if command != "buildbench" {
                get_size(&dir).unwrap()
            } else {
                0
            };
            let eps = i as f64 / elapsed.as_secs_f64();
            if benchreload {
                let reload_start = SystemTime::now().duration_since(UNIX_EPOCH).unwrap();
                replica.reload().unwrap();
                let reload_elapsed =
                    SystemTime::now().duration_since(UNIX_EPOCH).unwrap() - reload_start;
                let statm = procinfo::pid::statm_self().unwrap();
                println!("{},edits,{},ins,{},del,{},real_length,{},array_length,{},deltas,{},ms,{},eps,{},state_size,{},update_ms,{},commit_ms,{},reload_ms,{},statm.size,{},statm.resident,{},statm.share,{},statm.text,{},statm.data", i, insertions, deletions, insertions-deletions, length, deltas, elapsed.as_millis(), eps, state_size, update_elapsed.as_millis(), commit_elapsed.as_millis(), reload_elapsed.as_millis(), statm.size*page_size, statm.resident*page_size, statm.share*page_size, statm.text*page_size, statm.data*page_size);
            } else {
                let statm = procinfo::pid::statm_self().unwrap();
                println!("{},edits,{},ins,{},del,{},real_length,{},array_length,{},deltas,{},ms,{},eps,{},state_size,{},update_ms,{},commit_ms,{},reload_ms,{},statm.size,{},statm.resident,{},statm.share,{},statm.text,{},statm.data", i, insertions, deletions, insertions-deletions, length, deltas, elapsed.as_millis(), eps, state_size, update_elapsed.as_millis(), commit_elapsed.as_millis(), -1, statm.size*page_size, statm.resident*page_size, statm.share*page_size, statm.text*page_size, statm.data*page_size);
            }
        }
    } else if command == "read" || command == "readflate" {
        if args.len() < 3 {
            eprintln!("Missing directory");
            exit(1);
        }
        let dir = args[2].as_str();
        let reload_start = SystemTime::now().duration_since(UNIX_EPOCH).unwrap();
        let file_adapter: Box<dyn Adapter> = if command == "read" {
            Box::new(FilesystemAdapter::new(&dir).expect("cannot_initialize_adapter"))
        } else {
            Box::new(Flate2FilesystemAdapter::new(&dir).expect("cannot_initialize_adapter"))
        };
        let replica =
            Melda::new(Arc::new(RwLock::new(file_adapter))).expect("cannot_initialize_crdt");
        let reload_elapsed = SystemTime::now().duration_since(UNIX_EPOCH).unwrap() - reload_start;
        let reload_statm = procinfo::pid::statm_self().unwrap();
        let mut text = vec![];
        let read_start = SystemTime::now().duration_since(UNIX_EPOCH).unwrap();
        let data = replica.read().expect("failed_to_update");
        let read_elapsed = SystemTime::now().duration_since(UNIX_EPOCH).unwrap() - read_start;
        let read_statm = procinfo::pid::statm_self().unwrap();
        let root_obj = data.as_object().expect("not_an_object");
        if let Value::Array(characters) = root_obj.get(CHAR_KEY).unwrap() {
            for v in characters {
                let o = v.as_object().expect("expecting_character_object");
                let h = o
                    .get("#")
                    .expect("expecting_hash")
                    .as_str()
                    .expect("expecting_string");
                let c = u32::from_str_radix(h, 16).unwrap() as u8; // FIXME: this works because we don't expect non-ASCII chars
                text.push(c);
            }
        }
        println!("{}", String::from_utf8_lossy(&text));
        eprintln!("{},reload_ms,{},read_ms,{},reload_statm.size,{},reload_statm.resident,{},reload_statm.share,{},reload_statm.text,{},reload_statm.data,{},read_statm.size,{},read_statm.resident,{},read_statm.share,{},read_statm.text,{},read_statm.data",reload_elapsed.as_millis(), read_elapsed.as_millis(), reload_statm.size*page_size, reload_statm.resident*page_size, reload_statm.share*page_size, reload_statm.text*page_size, reload_statm.data*page_size,read_statm.size*page_size, read_statm.resident*page_size, read_statm.share*page_size, read_statm.text*page_size, read_statm.data*page_size);
    } else {
        eprintln!("Invalid command {}", command);
        exit(2);
    }
}
