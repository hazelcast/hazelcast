package com.hazelcast.dataseries.impl.aggregation;

import com.hazelcast.dataseries.AggregationRecipe;
import com.hazelcast.dataseries.impl.DataSeriesDataSerializerHook;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.spi.Operation;
import com.hazelcast.spi.OperationFactory;

import java.io.IOException;

public class PrepareAggregationOperationFactory implements OperationFactory {

    private String preparationId;
    private String name;
    private AggregationRecipe aggregationRecipe;

    public PrepareAggregationOperationFactory() {
    }

    public PrepareAggregationOperationFactory(String name,
                                              String preparationId,
                                              AggregationRecipe aggregationRecipe) {
        this.name = name;
        this.preparationId = preparationId;
        this.aggregationRecipe = aggregationRecipe;
    }

    @Override
    public Operation createOperation() {
        return new PrepareAggregationOperation(name, preparationId, aggregationRecipe);
    }

    @Override
    public int getFactoryId() {
        return DataSeriesDataSerializerHook.F_ID;
    }

    @Override
    public int getId() {
        return DataSeriesDataSerializerHook.PREPARE_AGGREGATION_OPERATION_FACTORY;
    }

    @Override
    public void writeData(ObjectDataOutput out) throws IOException {
        out.writeUTF(name);
        out.writeUTF(preparationId);
        out.writeObject(aggregationRecipe);
    }

    @Override
    public void readData(ObjectDataInput in) throws IOException {
        name = in.readUTF();
        preparationId = in.readUTF();
        aggregationRecipe = in.readObject();
    }
}