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
public class CollectionRollbackBackupOperation extends CollectionOperation implements BackupOperation {

    private long itemId;

    private boolean removeOperation;

    public CollectionRollbackBackupOperation() {
    }

    public CollectionRollbackBackupOperation(String name, long itemId, boolean removeOperation) {
        super(name);
        this.itemId = itemId;
        this.removeOperation = removeOperation;
    }

    public int getId() {
        return CollectionDataSerializerHook.COLLECTION_ROLLBACK_BACKUP;
    }

    public void beforeRun() throws Exception {

    }

    public void run() throws Exception {
        if (removeOperation){
            getOrCreateContainer().reserveRemoveBackup(itemId);
        } else {
            getOrCreateContainer().rollbackAddBackup(itemId);
        }
    }

    public void afterRun() throws Exception {

    }

    protected void writeInternal(ObjectDataOutput out) throws IOException {
        super.writeInternal(out);
        out.writeLong(itemId);
        out.writeBoolean(removeOperation);
    }

    protected void readInternal(ObjectDataInput in) throws IOException {
        super.readInternal(in);
        itemId = in.readLong();
        removeOperation = in.readBoolean();
    }
}
