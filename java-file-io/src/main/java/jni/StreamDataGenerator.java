package jni;

import org.apache.arrow.c.ArrowArray;
import org.apache.arrow.c.ArrowSchema;
import org.apache.arrow.c.Data;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.vector.VectorSchemaRoot;

public class StreamDataGenerator {
    private DataGenerator dataGenerator;
    private BufferAllocator allocator;
    private final int NUM_STREAM = 5;
    private int sentStream = 0;

    public StreamDataGenerator(BufferAllocator allocator) {
        this.dataGenerator = new DataGenerator(allocator);
        this.allocator = allocator;
    }

    // return two pointers for array and schema respectively from vectorschemaroot
    public long[] next() {
        System.out.println(
                "StreamDataGenerator next called. If you get two pointers with 0 values, that means stream has ended.");
        if (sentStream >= NUM_STREAM) {
            return new long[] { 0, 0 };
        }
        dataGenerator.generateData();
        VectorSchemaRoot root = dataGenerator.getVectorSchemaRoot();
        ArrowArray array = ArrowArray.allocateNew(allocator);
        ArrowSchema schema = ArrowSchema.allocateNew(allocator);
        Data.exportVectorSchemaRoot(allocator, root, null, array, schema);
        long[] arrarySchema = new long[2];
        arrarySchema[0] = array.memoryAddress();
        arrarySchema[1] = schema.memoryAddress();
        sentStream += 1;
        return arrarySchema;
    }

    public boolean isEnd() {
        return sentStream >= NUM_STREAM;
    }
}
