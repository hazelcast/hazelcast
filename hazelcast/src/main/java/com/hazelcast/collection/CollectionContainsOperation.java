package com.hazelcast.collection;

import com.hazelcast.collection.operation.CollectionOperation;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.Data;

import java.io.IOException;
import java.util.HashSet;
import java.util.Set;

/**
 * @ali 9/1/13
 */
public class CollectionContainsOperation extends CollectionOperation {

    Set<Data> valueSet;

    public CollectionContainsOperation() {
    }

    public CollectionContainsOperation(String name, Set<Data> valueSet) {
        super(name);
        this.valueSet = valueSet;
    }

    public int getId() {
        return CollectionDataSerializerHook.COLLECTION_CONTAINS;
    }

    public void beforeRun() throws Exception {

    }

    public void run() throws Exception {
        response = getOrCreateContainer().contains(valueSet);
    }

    public void afterRun() throws Exception {

    }

    protected void writeInternal(ObjectDataOutput out) throws IOException {
        super.writeInternal(out);
        out.writeInt(valueSet.size());
        for (Data value : valueSet) {
            value.writeData(out);
        }
    }

    protected void readInternal(ObjectDataInput in) throws IOException {
        super.readInternal(in);
        final int size = in.readInt();
        valueSet = new HashSet<Data>(size);
        for (int i=0; i<size; i++){
            final Data value = new Data();
            value.readData(in);
            valueSet.add(value);
        }
    }
}
