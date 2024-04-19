package jni;

import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.c.Data;

import org.apache.arrow.c.ArrowArray;
import org.apache.arrow.c.ArrowSchema;
import org.apache.arrow.memory.BufferAllocator;

import org.apache.arrow.memory.RootAllocator;

class LanceWriter {

    private static BufferAllocator allocator = new RootAllocator();

    // private static final int TEST_ROW_NUM = 1000000;

    static {
        System.loadLibrary("lance_file_jni");
    }

    private static native String hello(String input);

    private static native int write(String path, long arrayPtr, long schemaPtr);

    private static native int writeStream(String path, StreamDataGenerator streamDataGenerator);

    public static void write(String path, VectorSchemaRoot root) {
        // Print time before and after write

        long timestamp1 = System.currentTimeMillis();
        ArrowArray array = ArrowArray.allocateNew(allocator);
        ArrowSchema schema = ArrowSchema.allocateNew(allocator);
        Data.exportVectorSchemaRoot(allocator, root, null, array, schema);

        long timestamp2 = System.currentTimeMillis();
        write(path, array.memoryAddress(), schema.memoryAddress());
        long timestamp3 = System.currentTimeMillis();
        System.err.println("Write finished - JNI file writer interface");

        long jniTime = timestamp3 - timestamp2;
        long mallocTime = timestamp2 - timestamp1;
        System.out.println("Time used for file writing in JNI: " + jniTime + " milliseconds");
        System.out.println("Time used for allocating C struct: " + mallocTime + " milliseconds");
    }

    public static void main(String[] args) {
        System.out.println(hello("hello from Java!"));
        String homeDir = System.getenv("HOME");
        String base = homeDir + "/lance/file_jni_benchmark/java/";
        String fileName = "test_java.lance";
        // DataGenerator dataGenerator = new DataGenerator(allocator);
        // dataGenerator.generateData();
        // VectorSchemaRoot root = dataGenerator.getVectorSchemaRoot();

        // write(base + fileName, root);

        StreamDataGenerator streamDataGenerator = new StreamDataGenerator(allocator);
        writeStream(base + fileName, streamDataGenerator);
    }
}