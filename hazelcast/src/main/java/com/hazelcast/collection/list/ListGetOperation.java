package com.hazelcast.collection.list;

import com.hazelcast.collection.CollectionDataSerializerHook;
import com.hazelcast.collection.CollectionItem;
import com.hazelcast.collection.CollectionOperation;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;

import java.io.IOException;

/**
 * @ali 8/31/13
 */
public class ListGetOperation extends CollectionOperation {

    private int index;

    public ListGetOperation() {
    }

    public ListGetOperation(String name, int index) {
        super(name);
        this.index = index;
    }

    public void beforeRun() throws Exception {

    }

    public void run() throws Exception {
        final CollectionItem item = getOrCreateListContainer().get(index);
        response = item.getValue();
    }

    public void afterRun() throws Exception {

    }

    public int getId() {
        return CollectionDataSerializerHook.LIST_GET;
    }

    protected void writeInternal(ObjectDataOutput out) throws IOException {
        super.writeInternal(out);
        out.writeInt(index);
    }

    protected void readInternal(ObjectDataInput in) throws IOException {
        super.readInternal(in);
        index = in.readInt();
    }
}
