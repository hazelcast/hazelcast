package com.hazelcast.collection.list;

import com.hazelcast.collection.CollectionAddBackupOperation;
import com.hazelcast.collection.CollectionAddOperation;
import com.hazelcast.collection.CollectionDataSerializerHook;
import com.hazelcast.collection.CollectionItem;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.Data;
import com.hazelcast.spi.Operation;

import java.io.IOException;

/**
 * @ali 8/30/13
 */
public class ListAddOperation extends CollectionAddOperation {

    private int index = -1;

    public ListAddOperation() {
    }

    public ListAddOperation(String name, int index, Data value) {
        super(name, value);
        this.index = index;
    }

    public void run() throws Exception {
        final ListContainer container = getOrCreateListContainer();
        final CollectionItem item = container.add(index, value);
        if (item != null){
            itemId = item.getItemId();
            response = true;
        } else {
            response = false;
        }
    }

    public Operation getBackupOperation() {
        return new CollectionAddBackupOperation(name, itemId, value);
    }

    public int getId() {
        return CollectionDataSerializerHook.LIST_ADD;
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
