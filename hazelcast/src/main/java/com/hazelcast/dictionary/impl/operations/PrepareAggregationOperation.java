package com.hazelcast.dictionary.impl.operations;

import com.hazelcast.dictionary.AggregationRecipe;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;

import java.io.IOException;

import static com.hazelcast.dataseries.impl.DataSeriesDataSerializerHook.PREPARE_AGGREGATION;
import static com.hazelcast.dictionary.impl.DictionaryDataSerializerHook.PREPARE_AGGREGATION_OPERATION;

public class PrepareAggregationOperation extends DictionaryOperation {

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
        return PREPARE_AGGREGATION_OPERATION;
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
