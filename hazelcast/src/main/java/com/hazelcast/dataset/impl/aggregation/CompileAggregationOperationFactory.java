package com.hazelcast.dataset.impl.aggregation;

import com.hazelcast.dataset.AggregationRecipe;
import com.hazelcast.dataset.impl.DataSetDataSerializerHook;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.spi.Operation;
import com.hazelcast.spi.OperationFactory;

import java.io.IOException;

public class CompileAggregationOperationFactory implements OperationFactory {

    private String compileId;
    private String name;
    private AggregationRecipe aggregationRecipe;

    public CompileAggregationOperationFactory() {
    }

    public CompileAggregationOperationFactory(String name, String compileId, AggregationRecipe aggregationRecipe) {
        this.name = name;
        this.compileId = compileId;
        this.aggregationRecipe = aggregationRecipe;
    }

    @Override
    public Operation createOperation() {
        return new CompileAggregationOperation(name, compileId, aggregationRecipe);
    }

    @Override
    public int getFactoryId() {
        return DataSetDataSerializerHook.F_ID;
    }

    @Override
    public int getId() {
        return DataSetDataSerializerHook.COMPILE_AGGREGATION_OPERATION_FACTORY;
    }

    @Override
    public void writeData(ObjectDataOutput out) throws IOException {
        out.writeUTF(name);
        out.writeUTF(compileId);
        out.writeObject(aggregationRecipe);
    }

    @Override
    public void readData(ObjectDataInput in) throws IOException {
        name = in.readUTF();
        compileId = in.readUTF();
        aggregationRecipe = in.readObject();
    }
}