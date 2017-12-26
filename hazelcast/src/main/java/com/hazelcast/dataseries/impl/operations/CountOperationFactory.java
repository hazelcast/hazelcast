package com.hazelcast.dataseries.impl.operations;

import com.hazelcast.dataseries.impl.DataSeriesDataSerializerHook;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.spi.Operation;
import com.hazelcast.spi.OperationFactory;

import java.io.IOException;

public class CountOperationFactory implements OperationFactory {

    private String name;

    public CountOperationFactory() {
    }

    public CountOperationFactory(String name) {
        this.name = name;
    }

    @Override
    public Operation createOperation() {
        return new CountOperation(name);
    }

    @Override
    public int getFactoryId() {
        return DataSeriesDataSerializerHook.F_ID;
    }

    @Override
    public int getId() {
        return DataSeriesDataSerializerHook.COUNT_OPERATION_FACTORY;
    }

    @Override
    public void writeData(ObjectDataOutput out) throws IOException {
        out.writeUTF(name);
    }

    @Override
    public void readData(ObjectDataInput in) throws IOException {
        name = in.readUTF();
    }
}
