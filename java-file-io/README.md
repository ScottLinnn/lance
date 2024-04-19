# Lance-JNI

File-level read/write JNI bridge for [Lance](https://github.com/lancedb/lance).

[Lance](https://github.com/lancedb/lance) is a great data file format designed for modern ML workload. It's optimized for random access hence outperforms Parquet more than 100x on random reads performance. However, its interface is designed around the concept of dataset(Lance's counterpart of Iceberg), hiding the details of managing individual data files. In some cases, one might want to only use the Lance **file** format without its dataset management feature(e.g. user is already using Iceberg to manage Parquet files and wants to replace xx.parquet with xx.lance). To accommodate such demands, I developed the JNI bridge for Lance file read/write. It's designed for [Arrow](https://github.com/apache/arrow) in-memory format, so its primary usage is writing record batches to .lance files or reading record batches from .lance files.

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
