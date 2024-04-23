import os
import random
import shutil
import time
import pyarrow as pa
import lance


ROW_NUM = 40000000
NUM_TO_TAKE = 200


def generate_dummy_data():
    # Two columns, one int - id, one varchar - name

    id_list = [i for i in range(ROW_NUM)]
    name_list = [f"test{i}" for i in range(ROW_NUM)]

    id_array = pa.array(id_list)
    name_array = pa.array(name_list)

    schema = pa.schema(
        [
            pa.field("id", pa.int64()),
            pa.field("name", pa.string()),
        ]
    )

    record_batch = pa.RecordBatch.from_arrays([id_array, name_array], schema=schema)

    return record_batch


def producer():
    record_batch = generate_dummy_data()
    yield record_batch


def test_read_range():
    home_dir = os.environ.get("HOME")
    base = home_dir + "/lance/file_jni_benchmark/python/"
    dataset_url = "dataset_python.lance"

    start_time = round(time.time() * 1000)
    ds = lance.dataset(base + dataset_url)
    batches = ds.to_table(["id", "name"])
    end_time = round(time.time() * 1000)

    # row_num = 0
    # for batch in batches:
    #     row_num += batch.num_rows
    # print(f"Total rows: {row_num}")

    elapsed_time = end_time - start_time
    print("Read finished - Python native read_dataset interface")
    print(f"Time used for read: {elapsed_time} miliseconds")
    with open(base + "readRange.log", "w") as f:
        f.write(f"{elapsed_time}\n")
        f.write(f"{ROW_NUM}\n")


def test_read_random():
    home_dir = os.environ.get("HOME")
    base = home_dir + "/lance/file_jni_benchmark/python/"
    dataset_url = "dataset_python.lance"

    start_time = round(time.time() * 1000)
    ds = lance.dataset(base + dataset_url)
    indices = random.sample(range(ROW_NUM), NUM_TO_TAKE)

    data = ds.take(indices, columns=["id", "name"])
    end_time = round(time.time() * 1000)

    elapsed_time = end_time - start_time
    print("test_read_random finished")
    print(
        f"Time used for read {NUM_TO_TAKE} random indices: {elapsed_time} miliseconds"
    )
    # Write elapsed time and num to take in a file
    with open(base + "readIndex.log", "w") as f:
        f.write(f"{elapsed_time}\n")
        f.write(f"{NUM_TO_TAKE}\n")


def test_write():
    schema = pa.schema(
        [
            pa.field("id", pa.int64()),
            pa.field("name", pa.string()),
        ]
    )

    start_time = round(time.time() * 1000)
    home_dir = os.environ.get("HOME")
    base = home_dir + "/lance/file_jni_benchmark/python/"
    dataset_url = "dataset_python.lance"
    # If this dir exists, remove it
    if os.path.exists(base + dataset_url):
        shutil.rmtree(base + dataset_url)

    lance.write_dataset(producer(), base + dataset_url, schema)
    end_time = round(time.time() * 1000)

    elapsed_time = end_time - start_time
    print("Write finished - Python native write_dataset interface")
    print(f"Time used for write: {elapsed_time} miliseconds")

    with open(base + "write.log", "w") as f:
        f.write(f"{elapsed_time}\n")
        f.write(f"{ROW_NUM}\n")

    # Remove the dir of base


test_write()
test_read_range()
