# What is Melda?

Melda is a Delta-State JSON CRDT. CRDTs, which stand for Conflict-free Replicated Data Types, are data structures which can be replicated (copied) across multiple computers in a network. Each replica can be individually and concurrently updated without the need for central coordination or synchronization. Updates made on each replica can be merged at any time.

There exist different types of CRDTs: operation-based CRDTs (which generate and exchange update operations between replicas), state-based CRDTS (which exchange and merge the full state of each replica) and delta-state CRDT, such as Melda, (which exchange only the differences between versions, or states, of the data type).

Melda natively supports the JSON data format and provides a way to synchronize changes made to arbitrary JSON documents.

# How do I use Melda?

First of all, in **Cargo.toml** add the following dependency
```
melda = { git = "https://github.com/slashdotted/libmelda" }
```

Then import the required modules. For this example you will need:

```rust
use melda::{filesystemadapter::FilesystemAdapter, melda::Melda};
use serde_json::json;
```

To understand how to use Melda we consider the following situation, where a shared JSON document used by a fictitious activity planning software (i.e. a todo management software) is concurrently updated by multiple parties. The provided JSON is generated by the application (by serializing its data model). We assume that user **Alice** creates the first version of the shared JSON document, which will be named **v1_alice.json**. This first version contains the following data:

```json
{
	"software" : "MeldaDo",
	"version" : "1.0.0",
	"items♭" : []
}

```
The **root** object contains three fields: a **software** field which defines the name of the application, a **version** field, which sets the version of the software, and an **items♭** field, which maps to an array of JSON objects (one for each todo). Since this is the first version, the array of items is empty. The **♭** suffix is used to ask Melda to *flatten* the contents of the array, by extracting the contained JSON objects in order to keep track of their changes individually.

To better understand the purpose of the *flattening* procedure, consider how Melda processes the following two JSON files. The first one, named **v2_alice_noflat.json** contains:

```json
{
	"software" : "MeldaDo",
	"version" : "1.0.0",
	"items" : [
	   {"_id" : "alice_todo_01", "title" : "Buy milk", "description" : "Go to the grocery store"}
	]
}

```
In this case, Melda will keep the root object as is, and the changes made to the items array by one user will not merge with changes made by other users. So, for example, if two users add an element to the array on their replica and later merge those replicas, only one of the elements will be visible. On the contrary, consider now another version of the document, named **v2_alice.json**, which contains:

```json
{
	"software" : "MeldaDo",
	"version" : "1.0.0",
	"items♭" : [
	   {"_id" : "alice_todo_01", "title" : "Buy milk", "description" : "Go to the grocery store"}
	]
}

```
In this case the object within the **items♭** array will be extracted and tracked individually. In particular, two JSON objects results from the above document:
```json
{
	"_id" : "√",
	"software" : "MeldaDo",
	"version" : "1.0.0",
	"items♭" : [
	  "alice_todo_01"
	]
}

```
And the todo item itself:
```json
{
	"_id" : "alice_todo_01",
	"title" : "Buy milk",
	"description" : "Go to the grocery store"
}

```
Please notice that each object has its own unique identifier stored in the **_id** field. If an identifier is not provided by the client application, Melda will auto-generate one. The root object is always identified by **√** (this identifier cannot be changed by the client application). Since each object of the **items♭** array is tracked individually, if an user adds an element to the array and later merges his/her replica with another user all changes will be preserved.

If the collection of items becomes too large we can ask Melda to only store difference arrays between the newest revision of the document and the previous one. For that we simply need to prefix the key of the **items** field with the Δ character (greek capital letter delta). Version **delta_alice.json** might therefore become:
```json
{
	"software" : "MeldaDo",
	"version" : "1.0.0",
	"Δitems♭" : [
	   {"_id" : "alice_todo_01", "title" : "Buy milk", "description" : "Go to the grocery store"}
	]
}

```
To keep things simple, in the following we will not use difference arrays. Let's go back to our example situation...
Up until this point we only considered some JSON data, but we have yet to see how we can interact with Melda in order to update the data structure.

## Adapters

Melda implements a modular design where the logic of the CRDT is separated from the data storage. Storing the data (in our case, delta states) is achieved by means of **Adapters**. Melda already provides different types of adapters, supporting in-memory storage (**MemoryAdapter**), filesystem storage (**FilesystemAdapter**) and Solid Pods (**SolidAdapter**). Furthermore, it is possible to use a meta-adapter to compress data using the Flate2 algorithm (**Flate2Adapter**): such an adapter can be composed with other adapters.

We can initialize an adapter that will store data on the filesystem (in the **todolist** directory) as follows (**FilesystemAdapter**):
```rust
let adapter = Box::new(FilesystemAdapter::new("todolist").expect("Cannot initialize adapter"));
```

If we want to used compression we would add the **Flate2Adapter** as follows:
```rust
let adapter = Box::new(Flate2Adapter::new(Arc::new(RwLock::new(Box::new(
            FilesystemAdapter::new("todolist").expect("Cannot initialize adapter"))))));
```

## Initializing Melda

To initialize Melda we use the **new** method, passing the chosen adapter:
```rust
let mut m = Melda::new(Arc::new(RwLock::new(adapter))).expect("Failed to inizialize Melda");
```
Please note that we can remove the **mut** modifier if we only intend to read the CRDT.

## Updating the CRDT

In order to update the state of the CRDT we use the **update** method. First we need to parse the JSON data into a JSON value: since we use **serde_json** we call **serde_json::from_str** or the **json!** macro. Subsequently we call the **update** method on the resulting object:
```rust
let v = json!({ "software" : "MeldaDo", "version" : "1.0.0", "items♭" : []})
        .as_object()
        .expect("Not an object")
        .clone();
m.update(v).expect("Failed to update");

```
Updates made to the CRDT are now staged. In order to persist them we need to commit to the data storage backend. We commit using the **commit** method: we can pass an optional JSON object containing some additional information that will be stored along with the updates. Please note that it is possible to perform as many updates as needed before commiting, however it is not possible to commit if no updates have been made to the CRDT.
```rust
let info = json!({ "author" : "Alice", "description" : "First commit" })
	.as_object()
	.expect("Not an object")
	.clone();
let commit_result = m.commit(Some(info));
```
The result of the **commit** is either an error, **None** if there were no changes to be committed or **Some(String)** if changes were committed: the string is the commit identifier.
Upon success, on disk (in the **todolist** directory) the following content should have been created:
```
todolist/
├── 49
│   └── 49ccea4d5797250208edf9bc5d0b89edf23c30a61f5cb3fafb87069f07276a62.delta
└── b4
    └── b4e50e445542c4737f4cfd7a9193ffd3be3794049d361d114a44f36434257cb3.pack
```

The **.delta** file is called **delta block**, and contains the versioning information of each object in the CRDT, wherease the **.pack** file is the **data pack** which stores the actual JSON content of each object. Each commit produces a new delta block (with a different name, which corresponds to the hash digest of its content) and possibly a data pack (if new JSON values are produced). The directory structure of the **todolist** directory organizes files into sub-directories according to their prefix. 

We can perform another update using (again) the **update** method and commit the resulting changes:
```rust
let v = json!({ "software" : "MeldaDo", "version" : "1.0.0", "items♭" : [
       {"_id" : "alice_todo_01", "title" : "Buy milk", "description" : "Go to the grocery store"}
    ]
    })
    .as_object()
    .expect("Not an object")
    .clone();
m.update(v).expect("Failed to update");
let info = json!({ "author" : "Alice", "description" : "Add buy milk" })
        .as_object()
        .expect("Not an object")
        .clone();
let commit_result = m.commit(Some(info));
```

The changes will reflect on disk (with new packs and blocks created in the corresponding directories):
```
todolist/
├── 2b
│   └── 2b0a463fcba92d5cf7dae531a5c40b67aaa0f45ab351c15613534fb5bba28564.pack
├── 49
│   └── 49ccea4d5797250208edf9bc5d0b89edf23c30a61f5cb3fafb87069f07276a62.delta
├── b4
│   └── b4e50e445542c4737f4cfd7a9193ffd3be3794049d361d114a44f36434257cb3.pack
└── b6
    └── b6297035f06f13186160577099759dea843addcd1fbd05d24da87d9ac071da3b.delta
```
## Reading the data

At any time it is possible to read the state of the CRDT back into a JSON document using the **read** method:
```rust
let data = m.read().expect("Failed to read");
let content = serde_json::to_string(&data).unwrap();
println!("{}", content);
```

This additional code will print the following on the terminal:
```json
{"_id":"√","items♭":[{"_id":"alice_todo_01","description":"Go to the grocery store","title":"Buy milk"}],"software":"MeldaDo","version":"1.0.0"}
```

Each object managed by Melda will contain the **_id** field with the corresponding unique identifier.

## Sharing data

We now suppose that Alice shares the current state of the  **todolist** directory with Bob (she can simply zip the contents and send the compressed file by e-mail to Bob). We assume that Bob saves the contents in the **todolist_bob** directory. Bob initializes Melda and can perform some updates:
```rust
let adapter_bob =
        Box::new(FilesystemAdapter::new("todolist_bob").expect("Cannot initialize adapter"));
    let mut m_bob =
        Melda::new(Arc::new(RwLock::new(adapter_bob))).expect("Failed to inizialize Melda");
let v = json!({ "software" : "MeldaDo", "version" : "1.0.0", "items♭" : [
       {"_id" : "alice_todo_01", "title" : "Buy milk", "description" : "Go to the grocery store"},
       {"_id" : "bob_todo_01", "title" : "Pay bills", "description" : "Withdraw 500 to pay bill"},
       {"_id" : "bob_todo_02", "title" : "Call mom", "description" : "Call mom to schedule dinner"},
    ]
    })
    .as_object()
    .expect("Not an object")
    .clone();
m_bob.update(v).expect("Failed to update");
let info = json!({ "author" : "Bob", "description" : "Add some todos" })
        .as_object()
        .expect("Not an object")
        .clone();
let commit_result = m_bob.commit(Some(info));
```

As you might notice, two new items have been added by Bob. In the meantime, Alice continues to work on her replica, by removing one item (**alice_todo_01**) and adding a new item (**alice_todo_02**):
```rust
let v = json!({ "software" : "MeldaDo", "version" : "1.0.0", "items♭" : [
        {"_id" : "alice_todo_02", "title" : "Take picture of our dog", "description" : "It must be a nice one"}
     ]
     })
     .as_object()
     .expect("Not an object")
     .clone();
    m.update(v).expect("Failed to update");
let info = json!({ "author" : "Alice", "description" : "Some more stuff to do" })
        .as_object()
        .expect("Not an object")
        .clone();
let commit_result = m.commit(Some(info));
```

Finally, Bob shares his own copy with Alice: now Alice simply needs to merge the content of the directory (as received from Bob) with the local directory. Alternatively, suppose that the data modified by Bob is in the **todolist_bob** directory on Alice's computer. To merge changes back into the **todolist** directory, Alice can use the **meld** method:
```rust
let adapter_bob = Box::new(FilesystemAdapter::new("todolist_bob").expect("Cannot initialize adapter"));
let m_bob = Melda::new(Arc::new(RwLock::new(adapter_bob))).expect("Failed to inizialize Melda");
m.meld(&m_bob).expect(Failed to meld");
m.refresh();
```

The **refresh** method is used to load updates from the storage backend after the meld operation. Alice can then read the new state of the CRDT with:
```rust
let data = m.read().expect("Failed to read");
let content = serde_json::to_string(&data).unwrap();
println!("{}", content);
```

The result, printed on the terminal should look like:
```json
{"_id":"√","items♭":[{"_id":"bob_todo_01","description":"Withdraw 500 to pay bill","title":"Pay bills"},{"_id":"bob_todo_02","description":"Call mom to schedule dinner","title":"Call mom"},{"_id":"alice_todo_02","description":"It must be a nice one","title":"Take picture of our dog"}],"software":"MeldaDo","version":"1.0.0"}
```

As you can see, there is only one todo from Alice, as well as the two todos added by Bob.


# Benchmarks

In the [libmelda-benchmarks](https://github.com/slashdotted/libmelda-benchmarks) repository you will find a benchmark comparing Melda to Automerge

# Example integration

In the kibi directory you will find an example of integration of Melda into a text-editor. There is also another project [libmelda-tools](https://github.com/slashdotted/libmelda-tools/) which implements a simple command line tool to update, read, and meld Melda structures.

# Publications

## 2022
Amos Brocco "Melda: A General Purpose Delta State JSON CRDT". 9th Workshop on Principles and Practice of Consistency for Distributed Data (PaPoC'22). April 2022. (Accepted)

## 2021
Amos Brocco "Delta-State JSON CRDT: Putting Collaboration on Solid Ground". (Brief announcement). 23rd International Symposium on Stabilization, Safety, and Security of Distributed Systems (SSS 2021). November 2021. 

# License
(c)2021-2022 Amos Brocco,
GPL v3 (for now... but I will evaluate a change of license - to something like BSD3/MIT/... in the near future)
