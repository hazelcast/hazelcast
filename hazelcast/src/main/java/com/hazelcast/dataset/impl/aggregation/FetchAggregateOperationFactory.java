package com.hazelcast.dataset.impl.aggregation;

import com.hazelcast.dataset.impl.DataSetDataSerializerHook;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.spi.Operation;
import com.hazelcast.spi.OperationFactory;

import java.io.IOException;

public class FetchAggregateOperationFactory implements OperationFactory {

    private String name;
    private String aggregationId;

    public FetchAggregateOperationFactory() {
    }

    public FetchAggregateOperationFactory(String name, String aggregationId) {
        this.name = name;
        this.aggregationId = aggregationId;
    }

    @Override
    public Operation createOperation() {
        return new FetchAggregateOperation(name, aggregationId);
    }

    @Override
    public int getFactoryId() {
        return DataSetDataSerializerHook.F_ID;
    }

    @Override
    public int getId() {
        return DataSetDataSerializerHook.FETCH_AGGREGATOR_OPERATION_FACTORY;
    }

    @Override
    public void writeData(ObjectDataOutput out) throws IOException {
        out.writeUTF(name);
        out.writeUTF(aggregationId);
    }

    @Override
    public void readData(ObjectDataInput in) throws IOException {
        name = in.readUTF();
        aggregationId = in.readUTF();
    }
}
