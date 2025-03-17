use melda::{filesystemadapter::FilesystemAdapter, melda::Melda};
use serde_json::json;
use std::fs;
use std::io;
use std::path::Path;
use std::sync::{Arc, RwLock};

fn main() {
    // ensure clean state
    _ = std::fs::remove_dir_all("todolist_alice");
    _ = std::fs::remove_dir_all("todolist_bob");

    // create alice storage adapter
    let adapter_alice =
        Box::new(FilesystemAdapter::new("todolist_alice").expect("Cannot initialize adapter"));

    // initialize CRDT data structure
    let mut melda_alice =
        Melda::new(Arc::new(RwLock::new(adapter_alice))).expect("Failed to inizialize Melda");

    // create new JSON object
    let v = json!({ "software" : "MeldaDo", "version" : "1.0.0", "items♭" : []})
        .as_object()
        .expect("Not an object")
        .clone();

    // update CRDT with new json
    melda_alice.update(v).expect("Failed to update");

    // commit change
    let info = json!({ "author" : "Alice", "description" : "First commit" })
        .as_object()
        .expect("Not an object")
        .clone();
    let commit_result = melda_alice.commit(Some(info)).unwrap().unwrap();
    println!("alice made a commit: {:?}", commit_result);

    // create a new version of the json object
    let v = json!({ "software" : "MeldaDo", "version" : "1.0.0", "items♭":
        [
           {"_id" : "alice_todo_01", "title" : "Buy milk", "description": "Go to the grocery store"}
        ]
    })
    .as_object()
    .expect("Not an object")
    .clone();

    // perform another update
    melda_alice.update(v).expect("Failed to update");
    let info = json!({ "author" : "Alice", "description" : "Add buy milk" })
        .as_object()
        .expect("Not an object")
        .clone();

    // commit second change
    let commit_result = melda_alice.commit(Some(info)).unwrap().unwrap();
    println!("alice made another commit: {:?}", commit_result);

    // -- Read data
    let data = melda_alice.read(None).expect("Failed to read");
    let content = serde_json::to_string_pretty(&data).unwrap();
    println!("alice's current state{}", content);

    // sharing data data
    // copy alice to bob to create a second replica

    _ = copy_recursively("todolist_alice", "todolist_bob");

    let adapter_bob =
        Box::new(FilesystemAdapter::new("todolist_bob").expect("Cannot initialize adapter"));

    let melda_bob =
        Melda::new(Arc::new(RwLock::new(adapter_bob))).expect("Failed to inizialize Melda");

    // New version of the JSON object
    let v = json!(
        { "software" : "MeldaDo", "version" : "1.0.0", "items♭" : [
            {"_id" : "alice_todo_01", "title" : "Buy milk", "description" : "Go to the grocery store"},
            {"_id" : "bob_todo_01", "title" : "Pay bills", "description" : "Withdraw 500 to pay bill"},
            {"_id" : "bob_todo_02", "title" : "Call mom", "description" : "Call mom to schedule dinner"},
        ]
    })
    .as_object()
    .expect("Not an object")
    .clone();

    // update bob's replicat
    melda_bob.update(v).expect("Failed to update");
    let info = json!({ "author" : "Bob", "description" : "Add some todos" })
        .as_object()
        .expect("Not an object")
        .clone();

    // bob commit's the result
    let commit_result = melda_bob.commit(Some(info)).unwrap().unwrap();
    println!("bob made a commit: {:?}", commit_result);

    // meanwhile Alice continues to make changes...
    let v = json!({ "software" : "MeldaDo", "version" : "1.0.0", "items♭" :
        [
            {"_id" : "alice_todo_02", "title" : "Take picture of our dog", "description" : "It must be a nice one"}
        ]
        })
     .as_object()
     .expect("Not an object")
     .clone();

    melda_alice.update(v).expect("Failed to update");
    let info = json!({ "author" : "Alice", "description" : "Some more stuff to do" })
        .as_object()
        .expect("Not an object")
        .clone();
    let commit_result = melda_alice
        .commit(Some(info))
        .expect("commit failed")
        .unwrap();
    println!(
        "meanwhile alice made yet another commit: {:?}",
        commit_result
    );

    // Bob shares his own copy with Alice
    melda_alice.meld(&melda_bob).expect("Failed to meld");
    melda_alice.refresh().expect("failed to refresh");

    let data = melda_alice.read(None).expect("Failed to read");

    let content = serde_json::to_string_pretty(&data).unwrap();
    println!("alice pulled in bob's state");
    println!("{}", content);

    // Check for conflicts
    for uuid in melda_alice.in_conflict() {
        println!("{} has conflicts:", uuid);
        let winner = melda_alice.get_winner(&uuid).unwrap();
        let conflicting = melda_alice.get_conflicting(&uuid).unwrap();
        println!(
            "Winner: {:?} -> {:?}",
            winner,
            melda_alice.get_value(&uuid, Some(&winner))
        );
        for c in conflicting {
            println!("Conflict {:?}", melda_alice.get_value(&uuid, Some(&c)));
        }
    }

    // Resolve with winner
    println!("Resolving conflicts");
    for uuid in melda_alice.in_conflict() {
        let winner = melda_alice.get_winner(&uuid).unwrap();
        melda_alice
            .resolve_as(&uuid, &winner)
            .expect("Failed to resolve");
    }

    // Check for conflicts
    for uuid in melda_alice.in_conflict() {
        println!("{} has conflicts:", uuid);
        let winner = melda_alice.get_winner(&uuid).unwrap();
        let conflicting = melda_alice.get_conflicting(&uuid).unwrap();
        println!(
            "Winner: {:?} -> {:?}",
            winner,
            melda_alice.get_value(&uuid, Some(&winner))
        );
        for c in conflicting {
            println!("Conflict {:?}", melda_alice.get_value(&uuid, Some(&c)));
        }
    }

    // After resolution
    let data = melda_alice.read(None).expect("Failed to read");

    let content = serde_json::to_string_pretty(&data).unwrap();
    println!("alice final state");
    println!("{}", content);
}

pub fn copy_recursively(source: impl AsRef<Path>, destination: impl AsRef<Path>) -> io::Result<()> {
    fs::create_dir_all(&destination)?;
    for entry in fs::read_dir(source)? {
        let entry = entry?;
        let filetype = entry.file_type()?;
        if filetype.is_dir() {
            copy_recursively(entry.path(), destination.as_ref().join(entry.file_name()))?;
        } else {
            fs::copy(entry.path(), destination.as_ref().join(entry.file_name()))?;
        }
    }
    Ok(())
}
