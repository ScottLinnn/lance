use arrow_schema::Schema as ArrowSchema;

use jni::objects::{JClass, JIntArray, JString};
use jni::sys::{jint, jlong, jlongArray, jsize, jstring};
use jni::JNIEnv;

use lance_file::reader::FileReader;
use lance_io::object_store::ObjectStore;
use object_store::path::Path;
use tokio::runtime::Runtime;

use crate::export_array;
use crate::import_schema;

// Just a sample function to test bridge between Java and Rust
#[no_mangle]
pub extern "system" fn Java_jni_LanceReader_hello<'local>(
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
    let new_input = input + "Hello from Rust! I'm reader bridge!";
    // Then we have to create a new Java string to return. Again, more info
    // in the `strings` module.
    let output = env
        .new_string(new_input)
        .expect("Couldn't create java string!");

    // Finally, extract the raw pointer to return.
    output.into_raw()
}

#[no_mangle]
pub extern "system" fn Java_jni_LanceReader_readRangeJni<'local>(
    mut env: JNIEnv<'local>,
    _class: JClass<'local>,
    path: JString<'local>,
    start: jint,
    end: jint,
    schema_ptr: jlong,
) -> jlongArray {
    // Get the path string from the Java side
    let path_str: String = env.get_string(&path).unwrap().into();

    let mut record_batch = None;

    let store = ObjectStore::from_path(&path_str).unwrap().0;
    let path = Path::from(path_str);
    let rt = Runtime::new().unwrap();
    let ffi_schema = import_schema(schema_ptr);
    let arrow_schema = ArrowSchema::try_from(&ffi_schema).unwrap();
    let schema = lance_core::datatypes::Schema::try_from(&arrow_schema).unwrap();

    let curr_time = std::time::Instant::now();
    rt.block_on(async {
        let reader = FileReader::try_new(&store, &path, schema).await.unwrap();
        let rb = reader
            .read_range(start as usize..end as usize, reader.schema(), None)
            .await
            .unwrap();
        record_batch = Some(rb);
    });
    println!("Rust read_range time elapsed: {:?}", curr_time.elapsed());

    // Convert the record batch to a list of two-size array
    let array_list = export_array(record_batch.unwrap());
    let len = array_list.len();
    let arr_size = 2 * len;

    // init a i64 array with size arr_size
    let arr = env.new_long_array(arr_size as jsize).unwrap();

    // set the elements of the array
    let mut start = 0;
    for pair in array_list {
        env.set_long_array_region(&arr, start, &pair).unwrap();
        start += 2;
    }

    arr.as_raw()
}

#[no_mangle]
pub extern "system" fn Java_jni_LanceReader_readIndexJni<'local>(
    mut env: JNIEnv<'local>,
    _class: JClass<'local>,
    path: JString<'local>,
    indices: JIntArray<'local>,
    schema_ptr: jlong,
) -> jlongArray {
    // Get the path string from the Java side
    let path_str: String = env.get_string(&path).unwrap().into();

    // Get the indices array from the Java side
    let indices_vec: Vec<u32> = unsafe {
        let ae = env
            .get_array_elements(&indices, jni::objects::ReleaseMode::NoCopyBack)
            .unwrap();
        let len = ae.len();
        let mut vec = Vec::with_capacity(len as usize);
        let mut iter = ae.iter();
        for _ in 0..len {
            let val = iter.next().unwrap();
            vec.push(val.clone() as u32);
        }
        vec
    };

    let store = ObjectStore::from_path(&path_str).unwrap().0;
    let path = Path::from(path_str);
    let rt = Runtime::new().unwrap();
    let ffi_schema = import_schema(schema_ptr);
    let arrow_schema = ArrowSchema::try_from(&ffi_schema).unwrap();
    let schema = lance_core::datatypes::Schema::try_from(&arrow_schema).unwrap();
    let mut record_batch = None;
    rt.block_on(async {
        let reader = FileReader::try_new(&store, &path, schema).await.unwrap();
        record_batch = Some(
            reader
                .take(&indices_vec, reader.schema(), None)
                .await
                .unwrap(),
        );
    });

    // Convert the record batch to a list of two-size array
    let array_list = export_array(record_batch.unwrap());

    let len = array_list.len();
    let arr_size = 2 * len;

    // init a i64 array with size arr_size
    let arr = env.new_long_array(arr_size as jsize).unwrap();

    // set the elements of the array
    let mut start = 0;
    for pair in array_list {
        env.set_long_array_region(&arr, start, &pair).unwrap();
        start += 2;
    }

    arr.as_raw()
}
