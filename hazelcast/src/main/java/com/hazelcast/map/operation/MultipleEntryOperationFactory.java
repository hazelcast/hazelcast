package com.hazelcast.map.operation;

import com.hazelcast.map.EntryProcessor;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.Data;
import com.hazelcast.spi.Operation;
import com.hazelcast.spi.OperationFactory;
import java.io.IOException;
import java.util.HashSet;
import java.util.Set;

/**
 * date: 19/12/13
 * author: eminn
 */
public class MultipleEntryOperationFactory implements OperationFactory {

    private String name;
    private Set<Data> keys;
    private EntryProcessor entryProcessor;

    public MultipleEntryOperationFactory() {
    }

    public MultipleEntryOperationFactory(String name, Set<Data> keys, EntryProcessor entryProcessor) {
        this.name = name;
        this.keys = keys;
        this.entryProcessor = entryProcessor;
    }

    @Override
    public Operation createOperation() {
        return new MultipleEntryOperation(name, keys, entryProcessor);
    }

    @Override
    public void writeData(ObjectDataOutput out) throws IOException {
        out.writeUTF(name);
        out.writeInt(keys.size());
        for (Data key : keys) {
            key.writeData(out);
        }
        out.writeObject(entryProcessor);
    }

    @Override
    public void readData(ObjectDataInput in) throws IOException {
        this.name = in.readUTF();
        int size = in.readInt();
        this.keys = new HashSet<Data>(size);
        for (int i = 0; i < size; i++) {
            Data key = new Data();
            key.readData(in);
            keys.add(key);
        }
        this.entryProcessor = in.readObject();
    }
}
