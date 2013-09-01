package com.hazelcast.collection;

import com.hazelcast.collection.operation.CollectionOperation;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.spi.BackupOperation;

import java.io.IOException;

/**
 * @ali 8/31/13
 */
public class CollectionRemoveBackupOperation extends CollectionOperation implements BackupOperation {

    private long itemId;

    public CollectionRemoveBackupOperation() {
    }

    public CollectionRemoveBackupOperation(String name, long itemId) {
        super(name);
        this.itemId = itemId;
    }

    public void beforeRun() throws Exception {

    }

    public void run() throws Exception {
        getOrCreateContainer().removeBackup(itemId);
    }

    public void afterRun() throws Exception {

    }

    public int getId() {
        return CollectionDataSerializerHook.COLLECTION_REMOVE_BACKUP;
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
