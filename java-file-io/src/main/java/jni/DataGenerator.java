package jni;

import java.util.Arrays;
import java.util.List;

import org.apache.arrow.vector.types.pojo.Schema;

import org.apache.arrow.vector.IntVector;

import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.VarCharVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.types.pojo.Field;

import java.nio.charset.StandardCharsets;

public class DataGenerator {
    private Schema schema;
    private VectorSchemaRoot vectorSchemaRoot;
    private BufferAllocator allocator;
    private final int NUM_ROWS = 8000000;

    public DataGenerator(BufferAllocator allocator) {
        this.allocator = allocator;

    }

    public Schema getSchema() {
        return schema;
    }

    public VectorSchemaRoot getVectorSchemaRoot() {
        return vectorSchemaRoot;
    }

    public void generateData() {
        // Two columns, one int - id, one varchar - name
        // Generate 400 rows with random value

        IntVector intVector = new IntVector("id", this.allocator);
        VarCharVector varCharVector = new VarCharVector("name", this.allocator);
        intVector.allocateNew();
        varCharVector.allocateNew();

        for (int i = 0; i < NUM_ROWS; i++) {
            intVector.setSafe(i, 0 + i);
            varCharVector.setSafe(i, ("test" + i).getBytes(StandardCharsets.UTF_8));
        }
        intVector.setValueCount(NUM_ROWS);
        varCharVector.setValueCount(NUM_ROWS);

        List<Field> fields = Arrays.asList(intVector.getField(), varCharVector.getField());
        List<FieldVector> vectors = Arrays.asList((FieldVector) intVector, varCharVector);
        Schema schema = new Schema(fields);
        VectorSchemaRoot vectorSchemaRoot = new VectorSchemaRoot(fields, vectors);

        this.schema = schema;
        this.vectorSchemaRoot = vectorSchemaRoot;
    }

}
