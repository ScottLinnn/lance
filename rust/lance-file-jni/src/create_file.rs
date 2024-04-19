// use arrow::array::RecordBatch;
// use arrow::array::{Int32Array, StringArray};
// use arrow::datatypes::{DataType, Field, Schema};
// use lance::io::ObjectStore;
// use std::sync::Arc;

// fn create_sample_rb() -> RecordBatch {
//     let schema = Schema::new(vec![
//         Field::new("id", DataType::Int32, false),
//         Field::new("name", DataType::Utf8, false),
//     ]);
//     let ids = Int32Array::from(vec![1, 2, 3, 4, 5]);
//     let names = StringArray::from(vec!["Alice", "Bob", "Carol", "Dave", "Eve"]);
//     let batch =
//         RecordBatch::try_new(Arc::new(schema), vec![Arc::new(ids), Arc::new(names)]).unwrap();
//     batch
// }

// fn create_file() {
//     let batch = create_sample_rb();
//     // write to a lance file
//     let store = ObjectStore::memory();
//     let path = Path::from("/read_range");
//     let mut file_writer = FileWriter::<NotSelfDescribing>::try_new(
//         &store,
//         &path,
//         schema.clone(),
//         &Default::default(),
//     )
//     .await
//     .unwrap();
//     file_writer.write(&[batch]).await.unwrap();
//     file_writer.finish().await.unwrap();
// }
