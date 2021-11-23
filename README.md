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
To update the data structure call the *update* procedure:

```rust
let mut base_doc = json!({ }).as_object().unwrap().clone();
replica.update(base_doc.clone()).expect("failed_to_update");
```
You can perform as many updates as you want. Finally, you need to commit those changes:

```rust
replica.commit(None, false).expect("failed_to_commit");
```

To read the data structure back into a JSON document use the *read* procedure:
```rust
let data = replica.read().expect("failed_to_read");
let root_obj = data.as_object().expect("not_an_object");
```

# License
(c)2021 Amos Brocco,
GPL v3
