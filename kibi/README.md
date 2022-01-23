# INSTALLATION

1. Clone the kibi repository:

```bash
git clone https://github.com/ilai-deutel/kibi
```


2. Reset to commit 610b5edf
```bash
cd kibi
git reset --hard 610b5edf
```

3. Apply patch
```bash
patch -p1 < ../PATCH_AGAINST_KIBI_0.2.2_COMMIT_610b5edf.patch
```

4. Fetch the Melda library
```bash
git clone https://github.com/slashdotted/libmelda.git
```

# COMPILE
```bash
cargo build
```

# EXECUTE
./target/debug/kibi file://$(pwd)/../kibi-evaluation-hello-world/hellofork_v1
