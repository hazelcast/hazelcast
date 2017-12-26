package com.hazelcast.dataseries.impl.aggregation;

import com.hazelcast.dataseries.AggregationRecipe;
import com.hazelcast.dataseries.impl.operations.DataSeriesOperation;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;

import java.io.IOException;

import static com.hazelcast.dataseries.impl.DataSeriesDataSerializerHook.PREPARE_AGGREGATION;

public class PrepareAggregationOperation extends DataSeriesOperation {

    public AggregationRecipe aggregationRecipe;
    private String preparationId;

    public PrepareAggregationOperation() {
    }

    public PrepareAggregationOperation(String name, String preparationId, AggregationRecipe aggregationRecipe) {
        super(name);
        this.aggregationRecipe = aggregationRecipe;
        this.preparationId = preparationId;
    }

    @Override
    public int getId() {
        return PREPARE_AGGREGATION;
    }

    @Override
    public void run() throws Exception {
        partition.prepareAggregation(preparationId, aggregationRecipe);
    }

    @Override
    protected void writeInternal(ObjectDataOutput out) throws IOException {
        super.writeInternal(out);
        out.writeUTF(preparationId);
        out.writeObject(aggregationRecipe);
    }

    @Override
    protected void readInternal(ObjectDataInput in) throws IOException {
        super.readInternal(in);
        preparationId = in.readUTF();
        aggregationRecipe = in.readObject();
    }
}

