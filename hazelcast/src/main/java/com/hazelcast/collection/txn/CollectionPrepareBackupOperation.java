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
public class CollectionPrepareBackupOperation extends CollectionOperation implements BackupOperation {

    long itemId;

    boolean removeOperation;

    public CollectionPrepareBackupOperation() {
    }

    public CollectionPrepareBackupOperation(String name, long itemId, boolean removeOperation) {
        super(name);
        this.itemId = itemId;
        this.removeOperation = removeOperation;
    }

    public int getId() {
        return CollectionDataSerializerHook.COLLECTION_PREPARE_BACKUP;
    }

    public void beforeRun() throws Exception {

    }

    public void run() throws Exception {
        if (removeOperation){
            getOrCreateContainer().reserveRemoveBackup(itemId);
        } else {
            getOrCreateContainer().reserveAddBackup(itemId);
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
