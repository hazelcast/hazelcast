package com.hazelcast.collection.txn;

import com.hazelcast.collection.CollectionBackupAwareOperation;
import com.hazelcast.collection.CollectionDataSerializerHook;
import com.hazelcast.collection.CollectionItem;
import com.hazelcast.core.ItemEventType;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.Data;
import com.hazelcast.spi.Operation;

import java.io.IOException;

/**
 * @ali 9/3/13
 */
public class CollectionTxnRemoveOperation extends CollectionBackupAwareOperation {

    private long itemId;

    private transient CollectionItem item;

    public CollectionTxnRemoveOperation() {
    }

    public CollectionTxnRemoveOperation(String name, long itemId) {
        super(name);
        this.itemId = itemId;
    }

    public boolean shouldBackup() {
        return true;
    }

    public Operation getBackupOperation() {
        return new CollectionTxnRemoveBackupOperation(name, itemId);
    }

    public int getId() {
        return CollectionDataSerializerHook.COLLECTION_TXN_REMOVE;
    }

    public void beforeRun() throws Exception {

    }

    public void run() throws Exception {
        item = getOrCreateContainer().commitRemove(itemId);
    }

    public void afterRun() throws Exception {
        publishEvent(ItemEventType.REMOVED, (Data)item.getValue());
    }

    protected void writeInternal(ObjectDataOutput out) throws IOException {
        super.writeInternal(out);
        out.writeLong(itemId);
    }

    protected void readInternal(ObjectDataInput in) throws IOException {
        super.readInternal(in);
        itemId = in.readLong();
    }
}
