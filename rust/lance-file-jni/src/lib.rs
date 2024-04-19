pub mod reader;
pub mod writer;

use std::ops::Deref;

use arrow::array::{RecordBatch, RecordBatchReader, StructArray};
use arrow::ffi::{from_ffi, FFI_ArrowArray, FFI_ArrowSchema};
use arrow::ffi_stream::ArrowArrayStreamReader;
use arrow::ffi_stream::FFI_ArrowArrayStream;
use arrow_schema::Schema as ArrowSchema;

// Helper functions to handle dirty FFI things.

pub fn import_schema(ptr: i64) -> FFI_ArrowSchema {
    unsafe {
        let mut ffi_schema = FFI_ArrowSchema::empty();
        (ptr as *mut FFI_ArrowSchema).copy_to(&mut ffi_schema as *mut FFI_ArrowSchema, 1);

        return ffi_schema;
    }
}

pub fn import_array(ptr_arr: i64, ffi_schema: FFI_ArrowSchema) -> RecordBatch {
    unsafe {
        let mut ffi_array = FFI_ArrowArray::empty();
        (ptr_arr as *mut FFI_ArrowArray).copy_to(&mut ffi_array as *mut FFI_ArrowArray, 1);
        let array_data = from_ffi(ffi_array, &ffi_schema).unwrap();
        let struct_array = StructArray::from(array_data);
        RecordBatch::from(struct_array)
    }
}

pub fn import_stream(ptr_arr: i64) -> (ArrowArrayStreamReader, ArrowSchema) {
    unsafe {
        let mut ffi_stream = FFI_ArrowArrayStream::empty();
        (ptr_arr as *mut FFI_ArrowArrayStream)
            .copy_to(&mut ffi_stream as *mut FFI_ArrowArrayStream, 1);

        let sr =
            ArrowArrayStreamReader::from_raw(&mut ffi_stream as *mut FFI_ArrowArrayStream).unwrap();
        let schema = ArrowArrayStreamReader::schema(&sr);
        (sr, schema.deref().to_owned())
    }
}

pub fn export_array(rb: RecordBatch) -> Vec<[i64; 2]> {
    let mut vec = Vec::new();
    for i in 0..rb.num_columns() {
        let array = rb.column(i);
        let data = array.to_data();
        let out_array = FFI_ArrowArray::new(&data);
        let out_schema = FFI_ArrowSchema::try_from(data.data_type()).unwrap();

        let schema = Box::new(out_schema);
        let array = Box::new(out_array);
        let schema_addr = Box::into_raw(schema) as i64;
        let array_addr = Box::into_raw(array) as i64;

        vec.push([schema_addr, array_addr]);
    }
    vec
    //https://docs.rs/arrow/33.0.0/arrow/ffi/index.html
    //https://arrow.apache.org/docs/java/cdata.html#java-to-c
    //https://stackoverflow.com/questions/75527485/how-to-export-arrow-rs-array-to-java
    //https://github.com/apache/arrow-rs/blob/3761ac53cab55c269b06d9a13825dd81b03e0c11/arrow/src/ffi.rs#L579-L580
}
