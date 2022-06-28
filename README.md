# What is Melda?

Melda is a Delta-State JSON CRDT. CRDTs, which stand for Conflict-free Replicated Data Types, are data structures which can be replicated (copied) across multiple computers in a network. Each replica can be individually and concurrently updated without the need for central coordination or synchronization. Updates made on each replica can be merged at any time.

There exist different types of CRDTs: operation-based CRDTs (which generate and exchange update operations between replicas), state-based CRDTS (which exchange and merge the full state of each replica) and delta-state CRDT, such as Melda, (which exchange only the differences between versions, or states, of the data type).

Melda natively supports the JSON data format and provides a way to synchronize changes made to arbitrary JSON documents. You can work with Melda CRDTs either using this Rust library or using a [command line tool](https://github.com/slashdotted/libmelda-tools/). In the [Kibi w/Melda](https://github.com/slashdotted/kibi) repository you can find a fork of the original [Kibi](https://github.com/ilai-deutel/kibi) text editor with collaboration features implemented using Melda.


# How do I use Melda?

First of all, in **Cargo.toml** add the following dependency
```
melda = { git = "https://github.com/slashdotted/libmelda" }
```
or

```
melda = "0.1.15"
```

If using the [crate](https://crates.io/crates/melda)  from [crates.io](https://crates.io/crates/melda) adapt the version string as needed. Then import the required modules. For this example you will need:

```rust
use melda::{filesystemadapter::FilesystemAdapter, melda::Melda};
use serde_json::json;
use std::sync::{Arc, RwLock};
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

Melda implements a modular design where the logic of the CRDT is separated from the data storage. Storing the data (in our case, delta states) is achieved by means of **Adapters**. Melda already provides different types of adapters, supporting in-memory storage (**MemoryAdapter**), a folder in the filesystem (**FilesystemAdapter**), a SQLite database (**SQLiteAdapter**), and a Solid Pod (**SolidAdapter**). Furthermore, it is possible to use a meta-adapter to compress data using the Flate2 algorithm (**Flate2Adapter**): other adapters can be composed with the **Flate2Adapter** to store compressed data on the chosen backend.

We can initialize an adapter that will store data on the filesystem (in the **todolist** directory) as follows (**FilesystemAdapter**):
```rust
let adapter = Box::new(FilesystemAdapter::new("todolist").expect("Cannot initialize adapter"));
```

If we want to used compression we would add the **Flate2Adapter** as follows:
```rust
let adapter = Box::new(Flate2Adapter::new(Arc::new(RwLock::new(Box::new(
            FilesystemAdapter::new("todolist").expect("Cannot initialize adapter"))))));
```

Alternatively you can use the **get_adapter** function to initialize an adapter from an Url:
```rust
let adapter = get_adapter("file+flate://todolist").unwrap();
```

Valid schemes for the **get_adapter** function are:

| Storage type      | Example path                                              | Description |
| ----------------- | ------------------------------------------------------------- | -------------------------- |
| In memory (memory://)           | memory://                  | |
| In memory w/Deflate compression (memory+flate://)           | memory+flate://   |  |
| In memory w/Brotli compression (memory+brotli://)           | memory+brotli://   |  |
| Folder (file://)           | file://mycrdtdocument                   | The absolute path of a folder (can be on a network share) |
| Folder w/Deflate compression (file+flate://)           | file+flate://mycrdtdocument     | The absolute path of a folder (can be on a network share) |
| Folder w/Brotli compression (file+brotli://)           | file+brotli://mycrdtdocument     | The absolute path of a folder (can be on a network share) |
| [Solid](https://solidproject.org/) Pod (solid://)           | solid://anuser.solidcommunity.net/mycrdtdocument | The URL of a [Solid](https://solidproject.org/) Pod |
| [Solid](https://solidproject.org/) Pod w/Deflate compression (solid+flate://)            | solid+flate://anuser.solidcommunity.net/mycrdtdocument  | The URL of a [Solid](https://solidproject.org/) Pod |                                                      |
| [Solid](https://solidproject.org/) Pod w/Brotli compression (solid+brotli://)            | solid+brotli://anuser.solidcommunity.net/mycrdtdocument  | The URL of a [Solid](https://solidproject.org/) Pod |                                                      |
| SQLite (sqlite://)           | sqlite://mycrdtdocument                   | The name of the database is required (use **:memory:** for in-memory storage) |
| SQLite w/Deflate compression (sqlite+flate://)           | sqlite+flate://mycrdtdocument     | The name of the database is required (use **:memory:** for in-memory storage) |
| SQLite w/Brotli compression (sqlite+brotli://)           | sqlite+brotli://mycrdtdocument     | The name of the database is required (use **:memory:** for in-memory storage) |
 
For [Solid](https://solidproject.org/) Pod's access, a username and a password are required.

## Initializing Melda

To initialize Melda we use the **new** method, passing the chosen adapter:
```rust
let mut m = Melda::new(Arc::new(RwLock::new(adapter))).expect("Failed to inizialize Melda");
```
or you can use an Url
```rust
let mut m = Melda::new_from_url("file+flate://todolist").expect("Failed to inizialize Melda");
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
m.meld(&m_bob).expect("Failed to meld");
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

## Getting the commit history
When we commit to the CRDT a new delta block is created. Each block is linked to some parent block, so as to create a *chain* of blocks. Upon commit Melda looks for **anchor** blocks, which are the ones that are currently not referenced as parent by any other block. We can get the set of current anchors using **get_anchors**, so if Alice wants to get the anchors for her CRDT she can use:
```rust
let anchors = m.get_anchors();
```
When we commit, the anchor list will contain just the identifier of the committed block, whereas when we meld from another replica we could end up with multiple anchors (which correspond to different *branches* in the commit tree).
We can obtain additional information about each block using the **get_block** method. If Alice wants to fetch additional information about the anchors, she can run the following code:
```rust
let anchors = m.get_anchors();
for block_id in anchors {
	if let Some(block) = m.get_block(&block_id).expect("Failed to get block") {
	    let parents = block.parents;
	    let info = block.info;
	    let packs = block.packs;
	    println!("Block {}", block_id);
	    println!("\t Information: {:?}", info);
	    println!("\t Parents: {:?}", parents);
	    println!("\t Packs: {:?}", packs);
	}
}
```
The **parents** field contains the identifiers of parent blocks (it is **None** if there are no parents, i.e. we are at an origin block), whereas the optional **info** field corresponds to the commit information. Finally, the **packs** field contains the identifiers of the data packs generated during the commit (it is set to **None** if no new data was produced for that commit).

## Going back in time
It is possible to navigate through commits by means of the **reload_until** method. As an example, suppose that Alice wants to go back to the origin:
```rust
 let mut anchors: Vec<String> = m.get_anchors().into_iter().collect();
    let mut block_id = anchors.get(0).cloned();
    while block_id.is_some() {
        let block = m
            .get_block(block_id.as_ref().unwrap())
            .expect("Failed to get block")
            .unwrap();
        if let Some(parents) = block.parents {
            let parents: Vec<String> = parents.into_iter().collect();
            block_id = parents.get(0).cloned();
        } else {
            // We reached the origin
            break;
        }
    }
    m.reload_until(block_id.as_ref().unwrap())
        .expect("Failed to reload until origin");
```

As expected, reading the JSON document now shows the first version:
```json
{"_id":"√","items♭":[],"software":"MeldaDo","version":"1.0.0"}
```

Alice can go back to the last version by reloading the CRDT:
```rust
m.reload();
```

## Conflicts and resolution
When we meld two replicas that had some concurrent updates it is likely that conflicts arise. In our scenario, the root document (with identifier √) has a conflict since both Alice and Bob modified the todo items on their own replica (which was later merged). It is possible to check for conflicts by means of the **in_conflict** method, which returns a set of all objects with conflicts. If an object has a conflict we can retrieve conflicting revisions with the **get_conflicting** method. Melda chooses a *winning* revision using a deterministic algorithm. We can get the currently *winning* revision using the **get_winner** method. Finally we can view the value associated with a winner or a conflicting revision using the **get_value** method. Accordingly, Alice can get a summary of conflicting objects and the corresponding values with something like:
```rust
for uuid in  m.in_conflict() {
	let winner = m.get_winner(&uuid).unwrap();
	let conflicting = m.get_conflicting(&uuid).unwrap();
	println!("Winner: {:?} -> {:?}", winner, m.get_value(&uuid, &winner));
	for c in conflicting {
	    println!("Conflict {:?}", m.get_value(&uuid, &c));
	}
}
```

We can resolve a conflict using the **resolve_as** method. For example, if Alice wants to accept the current winner and resolve all conflicts, she can use the following code:
```rust
    for uuid in  m.in_conflict() {
        let winner = m.get_winner(&uuid).unwrap();
        m.resolve_as(&uuid, &winner).expect("Failed to resolve");
    }
    assert!(m.in_conflict().is_empty());
```

# Benchmarks

In the [libmelda-benchmarks](https://github.com/slashdotted/libmelda-benchmarks) repository you will find a benchmark comparing Melda to Automerge

# Example integration

In the [Kibi](https://github.com/slashdotted/kibi) repository you will find an example of integration of Melda into a text-editor. There is also another project [libmelda-tools](https://github.com/slashdotted/libmelda-tools/) which implements a simple command line tool to update, read, and meld Melda structures.

# Publications

## 2022
Amos Brocco "Melda: A General Purpose Delta State JSON CRDT". 9th Workshop on Principles and Practice of Consistency for Distributed Data (PaPoC'22). April 2022. (Accepted)

## 2021
Amos Brocco "Delta-State JSON CRDT: Putting Collaboration on Solid Ground". (Brief announcement). 23rd International Symposium on Stabilization, Safety, and Security of Distributed Systems (SSS 2021). November 2021. 

# Contact

amos _dot_ brocco _at_ supsi _dot_ ch

# License
(c)2021-2022 Amos Brocco,
GPL v3 (for now... but I will evaluate a change of license - to something like BSD3/MIT/... in the near future)
