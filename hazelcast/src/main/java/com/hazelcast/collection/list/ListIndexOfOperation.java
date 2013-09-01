package com.hazelcast.collection.list;

import com.hazelcast.collection.CollectionDataSerializerHook;
import com.hazelcast.collection.operation.CollectionOperation;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.Data;

import java.io.IOException;

/**
 * @ali 9/1/13
 */
public class ListIndexOfOperation extends CollectionOperation {

    private boolean last;

    private Data value;

    public ListIndexOfOperation() {
    }

    public ListIndexOfOperation(String name, boolean last, Data value) {
        super(name);
        this.last = last;
        this.value = value;
    }

    public int getId() {
        return CollectionDataSerializerHook.LIST_INDEX_OF;
    }

    public void beforeRun() throws Exception {

    }

    public void run() throws Exception {
        getOrCreateListContainer().indexOf(last, value);
    }

    public void afterRun() throws Exception {

    }

    protected void writeInternal(ObjectDataOutput out) throws IOException {
        super.writeInternal(out);
        out.writeBoolean(last);
        value.writeData(out);
    }

    protected void readInternal(ObjectDataInput in) throws IOException {
        super.readInternal(in);
        last = in.readBoolean();
        value = new Data();
        value.readData(in);
    }
}
