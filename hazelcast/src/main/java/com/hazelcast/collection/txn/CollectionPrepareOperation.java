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
public class CollectionPrepareOperation extends CollectionBackupAwareOperation {

    private long itemId = -1;

    boolean removeOperation;

    String transactionId;

    public CollectionPrepareOperation() {
    }

    public CollectionPrepareOperation(String name, long itemId, String transactionId, boolean removeOperation) {
        super(name);
        this.itemId = itemId;
        this.removeOperation = removeOperation;
        this.transactionId = transactionId;
    }

    public boolean shouldBackup() {
        return true;
    }

    public Operation getBackupOperation() {
        return new CollectionPrepareBackupOperation(name, itemId, transactionId, removeOperation);
    }

    public int getId() {
        return CollectionDataSerializerHook.COLLECTION_PREPARE;
    }

    public void beforeRun() throws Exception {

    }

    public void run() throws Exception {
        getOrCreateContainer().ensureReserve(itemId);
    }

    public void afterRun() throws Exception {

    }

    protected void writeInternal(ObjectDataOutput out) throws IOException {
        super.writeInternal(out);
        out.writeLong(itemId);
        out.writeBoolean(removeOperation);
        out.writeUTF(transactionId);
    }

    protected void readInternal(ObjectDataInput in) throws IOException {
        super.readInternal(in);
        itemId = in.readLong();
        removeOperation = in.readBoolean();
        transactionId = in.readUTF();
    }
}
