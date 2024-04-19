use arrow::array::RecordBatch;

use arrow_schema::Schema as ArrowSchema;

use async_trait::async_trait;
use jni::objects::{JClass, JLongArray, JObject, JString};
use jni::sys::{jint, jlong, jstring};
use jni::JNIEnv;
use lance_core::datatypes::Schema;
use lance_core::Result;
use lance_file::writer::FileWriter;
use lance_file::writer::ManifestProvider;
use lance_io::object_store::ObjectStore;
use lance_io::object_writer::ObjectWriter;
use object_store::path::Path;
use tokio::runtime::Runtime;

use crate::import_array;
use crate::import_schema;

// The design assumes schema is provided by external source(java side), so use NotSelfDescribing.
struct NotSelfDescribing {}

#[async_trait]
impl ManifestProvider for NotSelfDescribing {
    async fn store_schema(_: &mut ObjectWriter, _: &Schema) -> Result<Option<usize>> {
        Ok(None)
    }
}

// Just a sample function to test bridge between Java and Rust
#[no_mangle]
pub extern "system" fn Java_jni_LanceWriter_hello<'local>(
    mut env: JNIEnv<'local>,
    // This is the class that owns our static method. It's not going to be used,
    // but still must be present to match the expected signature of a static
    // native method.
    _class: JClass<'local>,
    input: JString<'local>,
) -> jstring {
    // First, we have to get the string out of Java. Check out the `strings`
    // module for more info on how this works.
    let input: String = env
        .get_string(&input)
        .expect("Couldn't get java string!")
        .into();
    let new_input = input + "Hello from Rust! I'm writer bridge!";
    // Then we have to create a new Java string to return. Again, more info
    // in the `strings` module.
    let output = env
        .new_string(new_input)
        .expect("Couldn't create java string!");

    // Finally, extract the raw pointer to return.
    output.into_raw()
}

#[no_mangle]
pub extern "system" fn Java_jni_LanceWriter_write<'local>(
    mut env: JNIEnv<'local>,
    _class: JClass<'local>,
    path: JString<'local>,
    array_ptr: jlong,
    schema_ptr: jlong,
) -> jint {
    print!("Rust Java_jni_LanceWriter_write called");
    let path_str: String = env.get_string(&path).unwrap().into();

    let ffi_schema = import_schema(schema_ptr);
    let arrow_schema = ArrowSchema::try_from(&ffi_schema).unwrap();
    print!("Rust side successfully decoded schema");

    let schema = lance_core::datatypes::Schema::try_from(&arrow_schema).unwrap();
    let record_batch = import_array(array_ptr, ffi_schema);
    let store = ObjectStore::local();
    let path = Path::from(path_str);
    let rt = Runtime::new().unwrap();

    rt.block_on(async {
        let mut file_writer = FileWriter::<NotSelfDescribing>::try_new(
            &store,
            &path,
            schema.clone(),
            &Default::default(),
        )
        .await
        .unwrap();
        file_writer.write(&[record_batch]).await.unwrap();
        file_writer.finish().await.unwrap();
    });
    return 1;
}

#[no_mangle]
pub extern "system" fn Java_jni_LanceWriter_writeStream<'local>(
    mut env: JNIEnv<'local>,
    _class: JClass<'local>,
    path: JString<'local>,
    stream_data_generator: JObject<'local>,
) -> jint {
    print!("Rust Java_jni_LanceWriter_writeStream called");
    // get current time in ms
    let curr_time = std::time::Instant::now();

    // Call the next() method on StreamDataGenerator
    let path_str: String = env.get_string(&path).unwrap().into();
    let store = ObjectStore::local();
    let path = Path::from(path_str);
    let rt = Runtime::new().unwrap();
    let (record_batch, schema) = next_batch(&mut env, &_class, &stream_data_generator).unwrap();

    rt.block_on(async {
        let mut file_writer = FileWriter::<NotSelfDescribing>::try_new(
            &store,
            &path,
            schema.clone(),
            &Default::default(),
        )
        .await
        .unwrap();
        file_writer.write(&[record_batch]).await.unwrap();

        while let Some((record_batch, _)) = next_batch(&mut env, &_class, &stream_data_generator) {
            file_writer.write(&[record_batch]).await.unwrap();
        }
        file_writer.finish().await.unwrap();
    });

    println!("Rust writeStream time elapsed: {:?}", curr_time.elapsed());
    return 1;
}

// Call Java class to generate next batch and get the pointers
fn next_batch(
    env: &mut JNIEnv,
    _class: &JClass,
    stream_data_generator: &JObject,
) -> Option<(RecordBatch, Schema)> {
    let is_end_result = env
        .call_method(stream_data_generator, "isEnd", "()Z", &[])
        .expect("Failed to call isEnd method");
    let is_end = is_end_result.z().unwrap();
    match is_end {
        true => return None,
        false => (),
    }

    let next_result = env
        .call_method(stream_data_generator, "next", "()[J", &[])
        .expect("Failed to call next method");

    let ptr_array = JLongArray::from(next_result.l().unwrap());

    let mut ptrs: [i64; 2] = [0; 2];
    let _ = env.get_long_array_region(ptr_array, 0, &mut ptrs);
    let arr_ptr = ptrs[0];
    let schema_ptr = ptrs[1];
    let ffi_schema = import_schema(schema_ptr);
    let arrow_schema = ArrowSchema::try_from(&ffi_schema).unwrap();

    let schema = Schema::try_from(&arrow_schema).unwrap();
    let record_batch = import_array(arr_ptr, ffi_schema);
    Some((record_batch, schema))
}
