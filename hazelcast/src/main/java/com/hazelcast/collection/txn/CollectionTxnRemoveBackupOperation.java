package com.hazelcast.collection.txn;

import com.hazelcast.collection.CollectionDataSerializerHook;
import com.hazelcast.collection.CollectionOperation;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.spi.BackupOperation;

import java.io.IOException;

/**
 * @ali 9/3/13
 */
public class CollectionTxnRemoveBackupOperation extends CollectionOperation implements BackupOperation {

    private long itemId;

    public CollectionTxnRemoveBackupOperation() {
    }

    public CollectionTxnRemoveBackupOperation(String name, long itemId) {
        super(name);
        this.itemId = itemId;
    }

    public int getId() {
        return CollectionDataSerializerHook.COLLECTION_TXN_REMOVE_BACKUP;
    }

    public void beforeRun() throws Exception {

    }

    public void run() throws Exception {
        getOrCreateContainer().commitRemoveBackup(itemId);
    }

    public void afterRun() throws Exception {

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
