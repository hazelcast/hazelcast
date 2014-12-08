package com.hazelcast.map.impl.operation;

import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.Data;
import com.hazelcast.spi.Operation;
import com.hazelcast.spi.OperationFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/**
 * Operation factory for load all operations.
 */
public class MapLoadAllOperationFactory implements OperationFactory {

    private String name;
    private List<Data> keys;
    private boolean replaceExistingValues;


    public MapLoadAllOperationFactory() {
        keys = Collections.emptyList();
    }

    public MapLoadAllOperationFactory(String name, List<Data> keys, boolean replaceExistingValues) {
        this.name = name;
        this.keys = keys;
        this.replaceExistingValues = replaceExistingValues;
    }

    @Override
    public Operation createOperation() {
        return new LoadAllOperation(name, keys, replaceExistingValues);
    }

    @Override
    public void writeData(ObjectDataOutput out) throws IOException {
        out.writeUTF(name);
        final int size = keys.size();
        out.writeInt(size);
        for (Data key : keys) {
            out.writeData(key);
        }
        out.writeBoolean(replaceExistingValues);
    }

    @Override
    public void readData(ObjectDataInput in) throws IOException {
        name = in.readUTF();
        final int size = in.readInt();
        if (size > 0) {
            keys = new ArrayList<Data>(size);
        }
        for (int i = 0; i < size; i++) {
            Data data = in.readData();
            keys.add(data);
        }
        replaceExistingValues = in.readBoolean();
    }
}
