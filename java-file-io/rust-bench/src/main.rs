use arrow::{array::*, datatypes::Schema};

use futures::stream::StreamExt;
use lance::dataset::{Dataset, WriteParams};
use std::{env, fs, sync::Arc, time::SystemTime};
use tokio::runtime::Runtime; // Import the Stream trait

const ROW_NUM: usize = 40_000_000;

fn generate_dummy_data() -> RecordBatch {
    let id_array = (0..ROW_NUM).map(|i| Some(i as i64)).collect::<Vec<_>>();
    let id_array = Int64Array::from(id_array);
    let name_array = StringArray::from(
        (0..ROW_NUM)
            .map(|i| Some(format!("test{}", i)))
            .collect::<Vec<_>>(),
    );

    let schema = Schema::new(vec![
        arrow::datatypes::Field::new("id", arrow::datatypes::DataType::Int64, false),
        arrow::datatypes::Field::new("name", arrow::datatypes::DataType::Utf8, false),
    ]);

    RecordBatch::try_new(
        Arc::new(schema.clone()),
        vec![Arc::new(id_array), Arc::new(name_array)],
    )
    .unwrap()
}

fn test_write() {
    let start_time = SystemTime::now();
    let base = format!(
        "{}/lance/file_jni_benchmark/rust/",
        env::var("HOME").unwrap()
    );
    let dataset_url = "dataset_rust.lance";

    // If this dir exists, remove it
    if fs::metadata(format!("{}{}", base, dataset_url)).is_ok() {
        fs::remove_dir_all(format!("{}{}", base, dataset_url)).unwrap();
    }

    let arrow_schema = Schema::new(vec![
        arrow::datatypes::Field::new("id", arrow::datatypes::DataType::Int64, false),
        arrow::datatypes::Field::new("name", arrow::datatypes::DataType::Utf8, false),
    ]);

    let schema = Arc::new(arrow_schema.clone());

    let batches = vec![generate_dummy_data()];
    // let batches = vec![RecordBatch::new_empty(schema.clone())];
    let reader = RecordBatchIterator::new(batches.into_iter().map(Ok), schema);

    let write_params = WriteParams::default();
    let rt = tokio::runtime::Runtime::new().unwrap();
    rt.block_on(async {
        Dataset::write(reader, &(base + dataset_url), Some(write_params))
            .await
            .unwrap();
        let end_time = SystemTime::now();
        let elapsed_time = end_time.duration_since(start_time).unwrap().as_millis();

        println!("Write finished - Rust native write_dataset interface");
        println!("Time used for write: {} milliseconds", elapsed_time);
    });
}

fn test_read_range() {
    let start_time = SystemTime::now();
    let base = format!(
        "{}/lance/file_jni_benchmark/rust/",
        env::var("HOME").unwrap()
    );
    let dataset_url = "dataset_rust.lance";
    let rt = Runtime::new().unwrap();
    rt.block_on(async {
        let dataset = Dataset::open(&format!("{}{}", base, dataset_url))
            .await
            .unwrap();
        let scanner = dataset.scan();
        let _: Vec<RecordBatch> = scanner
            .try_into_stream()
            .await
            .unwrap()
            .map(|b| b.unwrap())
            .collect::<Vec<RecordBatch>>()
            .await;

        let end_time = SystemTime::now();
        let elapsed_time = end_time.duration_since(start_time).unwrap().as_millis();

        println!("Read finished - Rust native read_dataset interface");
        println!("Time used for read: {} milliseconds", elapsed_time);
    });
}

fn test_read_random() {
    let start_time = SystemTime::now();
    let base = format!(
        "{}/lance/file_jni_benchmark/rust/",
        env::var("HOME").unwrap()
    );
    let dataset_url = "dataset_rust.lance";
    let rt = Runtime::new().unwrap();
    rt.block_on(async {
        let dataset = Dataset::open(&format!("{}{}", base, dataset_url))
            .await
            .unwrap();
        let scanner = dataset.scan();
        let _: Vec<RecordBatch> = scanner
            .try_into_stream()
            .await
            .unwrap()
            .map(|b| b.unwrap())
            .collect::<Vec<RecordBatch>>()
            .await;

        let end_time = SystemTime::now();
        let elapsed_time = end_time.duration_since(start_time).unwrap().as_millis();

        println!("Read finished - Rust native read_dataset interface");
        println!("Time used for read: {} milliseconds", elapsed_time);
    });
}

fn main() {
    test_write();
    test_read_range();
}
