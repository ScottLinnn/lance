import os
import matplotlib.pyplot as plt

# Define the interfaces and log files
interfaces = ["java", "rust", "python"]
home_dir = os.environ.get("HOME")
base = home_dir + "/lance/file_jni_benchmark/"
log_files = {
    "java": {
        "write": base + "java/write.log",
        "readRange": base + "java/readRange.log",
        "readIndex": base + "java/readIndex.log",
    },
    "rust": {
        "write": base + "rust/write.log",
        "readRange": base + "rust/readRange.log",
        "readIndex": base + "rust/readIndex.log",
    },
    "python": {
        "write": base + "python/write.log",
        "readRange": base + "python/readRange.log",
        "readIndex": base + "python/readIndex.log",
    },
}
BENCH_NUM_ROWS = os.environ.get("BENCH_NUM_ROWS")

BENCH_NUM_TAKE = int(os.environ.get("BENCH_NUM_TAKE"))


# Function to read time duration from log file
def read_time_from_log(log_file):
    with open(log_file, "r") as file:
        return float(file.readline().strip())


# Read times from log files
times = {
    interface: {
        op: read_time_from_log(log_files[interface][op])
        for op in ["write", "readRange", "readIndex"]
    }
    for interface in interfaces
}

# Create bar charts
operations = ["write", "readRange", "readIndex"]
for operation in operations:
    plt.figure(figsize=(8, 5))
    plt.bar(
        interfaces,
        [times[interface][operation] for interface in interfaces],
        color=["blue", "green", "red"],
    )
    plt.xlabel("Interfaces")
    plt.ylabel("Time (ms)")
    file_size = os.path.getsize(f"{base}java/test_java.lance") / 1024 / 1024
    title = f"Time for {operation} operation. Data size: {int(file_size)} MB "
    if operation == "readIndex":
        title += (
            f"\n(Taking {BENCH_NUM_TAKE} random indices from {BENCH_NUM_ROWS} rows)"
        )
    plt.title(title)
    plt.show()
    plt.savefig(f"{operation}.png")
