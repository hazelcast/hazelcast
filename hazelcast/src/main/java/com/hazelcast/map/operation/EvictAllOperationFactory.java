package com.hazelcast.map.operation;

import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.spi.Operation;
import com.hazelcast.spi.OperationFactory;

import java.io.IOException;

/**
 * Operation factory for evict all operations.
 */
public class EvictAllOperationFactory implements OperationFactory {

    private String name;

    public EvictAllOperationFactory() {
    }

    public EvictAllOperationFactory(String name) {
        this.name = name;
    }

    @Override
    public Operation createOperation() {
        return new EvictAllOperation(name);
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
