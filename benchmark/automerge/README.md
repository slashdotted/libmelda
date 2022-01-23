# Installation
```bash
npm install automerge@v0.14.2
```
(in the paper we use version 0.14.2, which produces JSON changesets so as to be comparable with Melda, which also uses JSON changesets)

# Creating the changeset
```bash
node create-changesets.js 100
```
(note: 100 is the size of the batch)

# Reading the changeset
```bash
node create-changesets.js 100
```

# Automerge Binary changesets
Automerge has, since version v1.0.1-preview, introduced a binary changeset format, which results in a more compact representation (at least in the considered text editing scenario):

| Batch size | Batches | Automerge (JSON) | Automerge (Binary) | Melda | Fullstate | Melda (gzip) |
|:----------:|---------|------------------|--------------------|-------|-----------|--------------|
|   10 ops   | 25978   | 56.5             | 4.2                | 83    | 1699.8    | 62           |
|   100 ops  | 2598    | 54               | 0.72               | 23.7  | 170       | 14           |
|  1000 ops  | 260     | 52.8             | 0.239              | 16.5  | 17        |  8.2         |
|  10000 ops | 26      | 51.5             | 0.165              | 15    | 1.7       |  7.3         |

(all sizes in Mbytes)

These changesets can be generated using create-changesets-binary.js (requires the latest version of automerge, install with *npm install automerge*).
With this binary format reading the state at the end of 2598 batches (batch size of 100 operations), takes about 25 seconds (on an Intel(R) Core(TM) i7-6500U), and uses at most 210 Mbytes of RAM.

Similar optimizations might be possible in Melda too (by getting rid of the JSON format used to serialize delta states), but at the moment we have chosen to keep a human-readable JSON format to ease verification and parsing of the data format. As a simple improvement, the table above reports the results obtained by gzipping the individual delta states (delta blocks, data packs, and indices). 
