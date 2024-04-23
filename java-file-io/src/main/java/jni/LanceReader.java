package jni;

import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.VectorUnloader;
import org.apache.arrow.vector.ipc.message.ArrowRecordBatch;
import org.apache.arrow.c.Data;
import org.apache.arrow.c.ArrowArray;
import org.apache.arrow.vector.types.pojo.Schema;
import org.apache.arrow.c.ArrowSchema;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.*;

import java.util.ArrayList;
import java.util.List;

class LanceReader {
    private final int NUM_ROWS = 4000000;
    // private long schemaPtr; // should we do this?
    private static BufferAllocator allocator = new RootAllocator();

    public static void printFieldVector(FieldVector vector) {
        int valueCount = vector.getValueCount();

        System.out.println("Printing FieldVector data:");

        for (int i = 0; i < valueCount; i++) {
            if (vector.isNull(i)) {
                System.out.println("null");
            } else {
                // Depending on the data type of the vector, you'll need to use different
                // methods
                if (vector instanceof IntVector) {
                    System.out.println(((IntVector) vector).get(i));
                } else if (vector instanceof Float8Vector) {
                    System.out.println(((Float8Vector) vector).get(i));
                } else if (vector instanceof VarCharVector) {
                    System.out.println(((VarCharVector) vector).getObject(i).toString());
                } else {
                    // Handle other data types as needed
                    System.out.println("Unsupported data type");
                }
            }
        }
    }

    static {
        System.loadLibrary("lance_file_jni");
    }

    private static native String hello(String input);

    private static native long[] readRangeJni(String path, int start, int end, long schemaPtr);

    private static native long[] readIndexJni(String path, int[] indices, long schemaPtr);

    public static ArrowRecordBatch readIndex(String path, int[] indices, ArrowSchema schema) {
        // Assuming you have a jobjectArray called result
        System.out.println("readIndex, path = " + path);
        long schemaPtr = schema.memoryAddress();
        long[] result = readIndexJni(path, indices, schemaPtr);
        List<FieldVector> vec = new ArrayList<>();

        // Get the length of the array
        int length = result.length;

        // Iterate over the array and print the values
        for (int i = 0; i < length; i += 2) {

            ArrowSchema arrowSchema = ArrowSchema.wrap(result[i]);
            ArrowArray array = ArrowArray.wrap(result[i + 1]);

            FieldVector fieldVector = Data.importVector(allocator, array, arrowSchema, null);
            System.out.println("readIndex, printing FieldVector data, i = " + i);
            printFieldVector(fieldVector);
            vec.add(fieldVector);

        }

        // Build the vector schema root
        VectorSchemaRoot root = new VectorSchemaRoot((List<FieldVector>) vec);
        // Load ArrowRecordBatch from root
        VectorUnloader unloader = new VectorUnloader(root);
        ArrowRecordBatch recordBatch = unloader.getRecordBatch();
        return recordBatch;
    }

    public static ArrowRecordBatch readRange(String path, int start, int end, ArrowSchema schema) {
        // Assuming you have a jobjectArray called result
        System.out.println("readRange, path = " + path);
        long schemaPtr = schema.memoryAddress();
        long timestamp1 = System.currentTimeMillis();
        long[] result = readRangeJni(path, start, end, schemaPtr);
        long timestamp2 = System.currentTimeMillis();
        List<FieldVector> vec = new ArrayList<>();

        // Get the length of the array
        int length = result.length;

        // Iterate over the array and print the values
        for (int i = 0; i < length; i += 2) {
            BufferAllocator allocator = new RootAllocator();

            ArrowSchema arrowSchema = ArrowSchema.wrap(result[i]);
            ArrowArray array = ArrowArray.wrap(result[i + 1]);
            FieldVector fieldVector = Data.importVector(allocator, array, arrowSchema, null);
            // System.out.println("readRange, printing FieldVector data, i = " + i);
            // printFieldVector(fieldVector);
            vec.add(fieldVector);

        }

        // Build the vector schema root
        VectorSchemaRoot root = new VectorSchemaRoot((List<FieldVector>) vec);

        // Load ArrowRecordBatch from root
        VectorUnloader unloader = new VectorUnloader(root);
        ArrowRecordBatch recordBatch = unloader.getRecordBatch();
        long timestamp3 = System.currentTimeMillis();
        System.out.println("Time used for file reading in JNI: " + (timestamp2 - timestamp1) + " milliseconds");
        System.out.println("Time used for loading from C struct: " + (timestamp3 - timestamp2) + " milliseconds");
        return recordBatch;
    }

    private void benchRange() {
        String homeDir = System.getenv("HOME");
        String base = homeDir + "/lance/file_jni_benchmark/java/";
        String fileName = "test_java.lance";
        DataGenerator dataGenerator = new DataGenerator(allocator);
        dataGenerator.generateData();
        Schema schema = dataGenerator.getSchema();

        ArrowSchema schema1 = ArrowSchema.allocateNew(allocator);
        Data.exportSchema(allocator, schema, null, schema1);

        long timestamp1 = System.currentTimeMillis();
        readRange(base + fileName, 0, NUM_ROWS - 1, schema1);
        long timestamp2 = System.currentTimeMillis();
        long timeUsed = timestamp2 - timestamp1;
        System.out.println("benchRange() Time used: " + (timeUsed) + " milliseconds");
        System.out.println("benchRange() NUM_ROWS: " + NUM_ROWS);
        try {
            java.io.FileWriter myWriter = new java.io.FileWriter(base + "readRange.log");
            myWriter.write(timeUsed + "\n");
            myWriter.write(NUM_ROWS + "\n");
            myWriter.close();
        } catch (Exception e) {
            System.out.println("An error occurred.");
            e.printStackTrace();
        }
    }

    private void benchIndex() {
        String homeDir = System.getenv("HOME");
        String base = homeDir + "/lance/file_jni_benchmark/java/";
        String fileName = "test_java.lance";
        DataGenerator dataGenerator = new DataGenerator(allocator);
        dataGenerator.generateData();
        Schema schema = dataGenerator.getSchema();

        ArrowSchema schema1 = ArrowSchema.allocateNew(allocator);
        Data.exportSchema(allocator, schema, null, schema1);
        int numToTake = 200;
        // Geneate {numToTake} random indices
        int[] indices = new int[numToTake];
        for (int i = 0; i < numToTake; i++) {
            indices[i] = (int) (Math.random() * NUM_ROWS);
        }
        // Record time
        long timestamp1 = System.currentTimeMillis();
        readIndex(base + fileName, indices, schema1);
        long timestamp2 = System.currentTimeMillis();
        long timeUsed = timestamp2 - timestamp1;
        System.out.println("benchIndex() Time used: " + (timeUsed) + " milliseconds");
        System.out.println("benchIndex() numToTake: " + numToTake);
        try {
            java.io.FileWriter myWriter = new java.io.FileWriter(base + "readIndex.log");
            myWriter.write(timeUsed + "\n");
            myWriter.write(numToTake + "\n");
            myWriter.close();
        } catch (Exception e) {
            System.out.println("An error occurred.");
            e.printStackTrace();
        }
    }

    // The rest is just regular ol' Java!
    public static void main(String[] args) {
        System.out.println(hello("Hello from Java! "));

    }
}
