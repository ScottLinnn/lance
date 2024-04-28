use arrow::{array::*, datatypes::Schema};
use futures::stream::StreamExt;
use lance::dataset::{Dataset, WriteParams};
use std::{env, fs, io::Write, sync::Arc, time::SystemTime};
use tokio::runtime::Runtime; // Import the Stream trait

fn generate_dummy_data() -> RecordBatch {
    let row_num: usize = env::var("BENCH_NUM_ROWS").unwrap().parse().unwrap();
    let id_array = (0..row_num).map(|i| Some(i as i64)).collect::<Vec<_>>();
    let id_array = Int64Array::from(id_array);
    let name_array = StringArray::from(
        (0..row_num)
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
        let start_time = SystemTime::now();
        Dataset::write(reader, &(base.clone() + dataset_url), Some(write_params))
            .await
            .unwrap();
        let elapsed_time = start_time.elapsed().unwrap().as_millis();

        println!("Write finished - Rust native write_dataset interface");
        println!("Time used for write: {} milliseconds", elapsed_time);
        let row_num: usize = env::var("BENCH_NUM_ROWS").unwrap().parse().unwrap();
        let mut file = fs::File::create(format!("{}write.log", base)).unwrap();
        file.write(format!("{}\n", elapsed_time).as_bytes())
            .unwrap();
        file.write(format!("{}\n", row_num).as_bytes()).unwrap();
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

        println!("Read finished - Rust native dataset.scan() interface");
        println!("Time used for read: {} milliseconds", elapsed_time);
        let row_num: usize = env::var("BENCH_NUM_ROWS").unwrap().parse().unwrap();
        let mut file = fs::File::create(format!("{}readRange.log", base)).unwrap();
        file.write(format!("{}\n", elapsed_time).as_bytes())
            .unwrap();
        file.write(format!("{}\n", row_num).as_bytes()).unwrap();
    });
}

fn test_read_random() {
    let start_time = SystemTime::now();
    let base = format!(
        "{}/lance/file_jni_benchmark/rust/",
        env::var("HOME").unwrap()
    );
    let dataset_url = "dataset_rust.lance";
    let num_take: usize = env::var("BENCH_NUM_TAKE").unwrap().parse().unwrap();
    let rt = Runtime::new().unwrap();
    rt.block_on(async {
        let dataset = Dataset::open(&format!("{}{}", base, dataset_url))
            .await
            .unwrap();
        // Create {num_take} random indices
        let row_indices = (0..num_take)
            .map(|_| {
                let row_num: usize = env::var("BENCH_NUM_ROWS").unwrap().parse().unwrap();
                let index: usize = rand::random::<usize>() % row_num;
                index as u64
            })
            .collect::<Vec<_>>();
        let lance_schema = dataset.schema().clone();
        let _: RecordBatch = dataset.take(&row_indices[..], &lance_schema).await.unwrap();

        let end_time = SystemTime::now();
        let elapsed_time = end_time.duration_since(start_time).unwrap().as_millis();

        println!("Read finished - Rust native dataset.take() interface");
        println!("Time used for read: {} milliseconds", elapsed_time);

        // Write to a log file {base}readIndex.log
        let row_num: usize = env::var("BENCH_NUM_ROWS").unwrap().parse().unwrap();
        let mut file = fs::File::create(format!("{}readIndex.log", base)).unwrap();
        file.write(format!("{}\n", elapsed_time).as_bytes())
            .unwrap();
        file.write(format!("{}\n", row_num).as_bytes()).unwrap();
    });
}

fn main() {
    test_write();
    test_read_range();
    test_read_random();
}
