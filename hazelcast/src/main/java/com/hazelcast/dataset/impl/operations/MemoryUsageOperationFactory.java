package com.hazelcast.dataset.impl.operations;

import com.hazelcast.dataset.impl.DataSetDataSerializerHook;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.spi.Operation;
import com.hazelcast.spi.OperationFactory;

import java.io.IOException;

public class MemoryUsageOperationFactory implements OperationFactory {

    private String name;

    public MemoryUsageOperationFactory() {
    }

    public MemoryUsageOperationFactory(String name) {
        this.name = name;
    }

    @Override
    public Operation createOperation() {
        return new MemoryUsageOperation(name);
    }

    @Override
    public int getFactoryId() {
        return DataSetDataSerializerHook.F_ID;
    }

    @Override
    public int getId() {
        return DataSetDataSerializerHook.MEMORY_USAGE_OPERATION_FACTORY;
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