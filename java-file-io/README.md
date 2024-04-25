# Lance-JNI

File-level read/write JNI bridge for Lance.

Lance is a great data file format designed for modern ML workload. It's optimized for random access hence outperforms Parquet more than 100x on random read performance. However, its interface is designed around the concept of dataset(Lance's counterpart of Iceberg), hiding the details of managing individual data files. In some cases, one might want to only use the Lance **file** format without its dataset management feature(e.g. user is already using Iceberg to manage Parquet files and wants to replace xx.parquet with xx.lance). To accommodate such demands, I developed the JNI bridge for Lance file read/write. It's designed for [Arrow](https://github.com/apache/arrow) in-memory format, so its primary usage is writing record batches(in Java) to .lance files or reading record batches(in Java) from .lance files.

# Interfaces

In this implementation, the index(start index and end index for range read, index for random read) means the offset within a file, rather than something like a global id.

## Scan: `LanceReader.readRange()`

Read a file by range, range is specified by a start and end index. 

## Random Read: `LanceReader.readIndex()`

Read a file randomly, you can pass an array of indices and get the rows at all indices as one record batch.

## Write: `LanceWriter.write()`

Write a file with a record batch. 

In Java, there is not a perfect counterpart of Rusty `RecordBatch`. The closest one should be `VectorSchemaRoot`(see [doc](https://arrow.apache.org/docs/java/vector_schema_root.html)), so my write interfaces are actually taking `VectorSchemaRoot`s and exporting `RecordBatch`es out of them through some tricky Java thing.

## Streaming Write: `LanceWriter.writeStream()`

Write a file with multiple record batches. This is somewhat similar to a pull-based query operator - next batch is sent from Java to Rust when `next` is called in Rust.

# Run benchmark

Currently, the implementation assumes lance will be put in home directory (the path to lance is `$HOME/lance`).

Firstly we need to set up some environment viriables.

```
export LD_LIBRARY_PATH=/home/scott/lance/target/debug/ # the path to rust compiled shared library for JNI to load, replace "/home/scott/" with your path to lance directory
export BENCH_NUM_ROWS=40000000 # total number of rows in a Lance file
export BENCH_NUM_TAKE=2000 # total number of random rows taken from a file, for random read benchmark
```

Then we need to compile the shared library.

```
cargo build # Run this in the root directory of lance
```

We also need to install lance interface for python benchmark.

```
pip install pylance
```

Now we should be able to run the benchmarks. 

To run Python benchmark:
```
python benchmark.py
```
To run Rust benchmark:
```
cd rust-bench
cargo run
```
To run Java benchmark, simply run the `main` in `src/main/java/jni/LanceReader`.java and `src/main/java/jni/LanceWriter.java`. (I'm currently doing this by clicking the buttons in vscode)

After running the benchmarks for three interfaces, the logs will be available in `~/lance/file_jni_benchmark/`. Run

```
python plot.py
```

it will automatically read the logs and make the plots.

## Table data

Currently, the data the benchmarks are creating is two-column: an integer column and a string column.



