# melda
Melda is a Delta-State JSON CRDT

# Example usage

In Cargo.toml add the following dependency
```
melda = { git = "https://github.com/slashdotted/libmelda" }
```

Import the required modules

```rust
use melda::{melda::Melda, adapter::Adapter, filesystemadapter::FilesystemAdapter};
```

You can create an instance of a Melda data structure with a filesystem backend as follows:
```rust
let dir = "... path to a folder ...";
let file_adapter : Box<dyn Adapter> = Box::new(FilesystemAdapter::new(&dir).expect("cannot_initialize_adapter"));
let mut replica = Melda::new(Arc::new(RwLock::new(file_adapter))).expect("cannot_initialize_crdt");
```
Other available backends are MemoryAdapter (which stores data in RAM), SolidAdapter (which uses a Solid Pod), and SQLiteAdapter (which stores data into a SQLite database file). To update the data structure call the *update* procedure:

```rust
let mut base_doc = json!({ }).as_object().unwrap().clone();
replica.update(base_doc.clone()).expect("failed_to_update");
```
You can perform as many updates as you want.

```rust
let mut newer_version = json!({ "somekey" : [ "somedata", 1, 2, 3, 4 ] }).as_object().unwrap().clone();
replica.update(newer_version.clone()).expect("failed_to_update");
```

Finally, you need to commit those changes:

```rust
replica.commit(None, false).expect("failed_to_commit");
```
Upon commit a delta state will be written on the backend adapter (in this example, a *delta* and a *pack* file will be written to disk).
To read the data structure back into a JSON document use the *read* procedure:
```rust
let data = replica.read().expect("failed_to_read");
let root_obj = data.as_object().expect("not_an_object");
```

# Benchmarks

In the benchmark directory you will find a benchmark comparing Melda to Automerge

# Example integration

In the kibi directory you will find an example of integration of Melda into a text-editor. There is also another project [libmelda-tools](https://github.com/slashdotted/libmelda-tools/) which implements a simple command line tool to update, read, and meld Melda structures.

# Publications

## 2022
Amos Brocco "Melda: A General Purpose Delta State JSON CRDT". 9th Workshop on Principles and Practice of Consistency for Distributed Data (PaPoC'22). April 2022. (Accepted)

## 2021
Amos Brocco "Delta-State JSON CRDT: Putting Collaboration on Solid Ground". (Brief announcement). 23rd International Symposium on Stabilization, Safety, and Security of Distributed Systems (SSS 2021). November 2021. 

# License
(c)2021 Amos Brocco,
GPL v3
