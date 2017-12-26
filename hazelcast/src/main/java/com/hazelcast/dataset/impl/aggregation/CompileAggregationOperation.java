package com.hazelcast.dataset.impl.aggregation;

import com.hazelcast.dataset.AggregationRecipe;
import com.hazelcast.dataset.impl.Partition;
import com.hazelcast.dataset.impl.operations.DataSetOperation;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;

import java.io.IOException;

import static com.hazelcast.dataset.impl.DataSetDataSerializerHook.COMPILE_AGGREGATION;

public class CompileAggregationOperation extends DataSetOperation {

    public AggregationRecipe aggregationRecipe;
    private String compileId;

    public CompileAggregationOperation() {
    }

    public CompileAggregationOperation(String name, String compileId, AggregationRecipe aggregationRecipe) {
        super(name);
        this.aggregationRecipe = aggregationRecipe;
        this.compileId = compileId;
    }

    @Override
    public int getId() {
        return COMPILE_AGGREGATION;
    }

    @Override
    public void run() throws Exception {
        partition.compileAggregation(compileId, aggregationRecipe);
    }

    @Override
    protected void writeInternal(ObjectDataOutput out) throws IOException {
        super.writeInternal(out);
        out.writeUTF(compileId);
        out.writeObject(aggregationRecipe);
    }

    @Override
    protected void readInternal(ObjectDataInput in) throws IOException {
        super.readInternal(in);
        compileId = in.readUTF();
        aggregationRecipe = in.readObject();
    }
}

