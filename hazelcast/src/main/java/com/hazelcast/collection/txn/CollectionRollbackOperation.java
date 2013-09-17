package com.hazelcast.collection.txn;

import com.hazelcast.collection.CollectionBackupAwareOperation;
import com.hazelcast.collection.CollectionDataSerializerHook;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.spi.Operation;

import java.io.IOException;

/**
 * @ali 9/3/13
 */
public class CollectionRollbackOperation extends CollectionBackupAwareOperation {

    private long itemId;

    private boolean removeOperation;

    public CollectionRollbackOperation() {
    }

    public CollectionRollbackOperation(String name, long itemId, boolean removeOperation) {
        super(name);
        this.itemId = itemId;
        this.removeOperation = removeOperation;
    }

    public boolean shouldBackup() {
        return true;
    }

    public Operation getBackupOperation() {
        return new CollectionRollbackBackupOperation(name, itemId, removeOperation);
    }

    public int getId() {
        return CollectionDataSerializerHook.COLLECTION_ROLLBACK;
    }

    public void beforeRun() throws Exception {

    }

    public void run() throws Exception {
        if (removeOperation){
            getOrCreateContainer().rollbackRemove(itemId);
        }
        else {
            getOrCreateContainer().rollbackAdd(itemId);
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
