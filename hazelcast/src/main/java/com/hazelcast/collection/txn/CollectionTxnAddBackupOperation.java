package com.hazelcast.collection.txn;

import com.hazelcast.collection.CollectionDataSerializerHook;
import com.hazelcast.collection.CollectionOperation;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.Data;
import com.hazelcast.spi.BackupOperation;

import java.io.IOException;

/**
 * @ali 9/3/13
 */
public class CollectionTxnAddBackupOperation extends CollectionOperation implements BackupOperation {

    private long itemId;

    private Data value;

    public CollectionTxnAddBackupOperation() {
    }

    public CollectionTxnAddBackupOperation(String name, long itemId, Data value) {
        super(name);
        this.itemId = itemId;
        this.value = value;
    }

    public int getId() {
        return CollectionDataSerializerHook.COLLECTION_TXN_ADD_BACKUP;
    }

    public void beforeRun() throws Exception {

    }

    public void run() throws Exception {
        getOrCreateContainer().commitAddBackup(itemId, value);
        response = true;
    }

    public void afterRun() throws Exception {

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
