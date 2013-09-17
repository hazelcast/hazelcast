package com.hazelcast.collection.txn;

import com.hazelcast.collection.CollectionBackupAwareOperation;
import com.hazelcast.collection.CollectionDataSerializerHook;
import com.hazelcast.core.ItemEventType;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.Data;
import com.hazelcast.spi.Operation;

import java.io.IOException;

/**
 * @ali 9/3/13
 */
public class CollectionTxnAddOperation extends CollectionBackupAwareOperation {

    private long itemId;

    private Data value;

    public CollectionTxnAddOperation() {
    }

    public CollectionTxnAddOperation(String name, long itemId, Data value) {
        super(name);
        this.itemId = itemId;
        this.value = value;
    }

    public boolean shouldBackup() {
        return false;
    }

    public Operation getBackupOperation() {
        return new CollectionTxnAddBackupOperation(name, itemId, value);
    }

    public int getId() {
        return CollectionDataSerializerHook.COLLECTION_TXN_ADD;
    }

    public void beforeRun() throws Exception {

    }

    public void run() throws Exception {
        getOrCreateContainer().commitAdd(itemId, value);
        response = true;
    }

    public void afterRun() throws Exception {
        publishEvent(ItemEventType.ADDED, value);
    }

    protected void writeInternal(ObjectDataOutput out) throws IOException {
        super.writeInternal(out);
        out.writeLong(itemId);
        value.writeData(out);
    }

    protected void readInternal(ObjectDataInput in) throws IOException {
        super.readInternal(in);
        itemId = in.readLong();
        value = new Data();
        value.readData(in);
    }
}
