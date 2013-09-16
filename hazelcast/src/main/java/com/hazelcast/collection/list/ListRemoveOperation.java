package com.hazelcast.collection.list;

import com.hazelcast.collection.CollectionDataSerializerHook;
import com.hazelcast.collection.CollectionItem;
import com.hazelcast.collection.CollectionRemoveBackupOperation;
import com.hazelcast.collection.CollectionBackupAwareOperation;
import com.hazelcast.core.ItemEventType;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.Data;
import com.hazelcast.spi.Operation;

import java.io.IOException;

/**
 * @ali 9/1/13
 */
public class ListRemoveOperation extends CollectionBackupAwareOperation {

    private int index;

    private transient long itemId;

    public ListRemoveOperation() {
    }

    public ListRemoveOperation(String name, int index) {
        super(name);
        this.index = index;
    }

    public boolean shouldBackup() {
        return true;
    }

    public Operation getBackupOperation() {
        return new CollectionRemoveBackupOperation(name, itemId);
    }

    public int getId() {
        return CollectionDataSerializerHook.LIST_REMOVE;
    }

    public void beforeRun() throws Exception {
        publishEvent(ItemEventType.ADDED, (Data)response);
    }

    public void run() throws Exception {
        final CollectionItem item = getOrCreateListContainer().remove(index);
        itemId = item.getItemId();
        response = item.getValue();
    }

    public void afterRun() throws Exception {

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
