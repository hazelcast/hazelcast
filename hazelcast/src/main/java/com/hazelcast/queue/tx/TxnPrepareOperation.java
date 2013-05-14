package com.hazelcast.queue.tx;

import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.queue.QueueBackupAwareOperation;
import com.hazelcast.queue.QueueDataSerializerHook;
import com.hazelcast.spi.Operation;

import java.io.IOException;

/**
 * @ali 3/27/13
 */
public class TxnPrepareOperation extends QueueBackupAwareOperation {

    long itemId;

    boolean pollOperation;

    public TxnPrepareOperation() {
    }

    public TxnPrepareOperation(String name, long itemId, boolean pollOperation) {
        super(name);
        this.itemId = itemId;
        this.pollOperation = pollOperation;
    }

    public void run() throws Exception {
        response = getOrCreateContainer().txnEnsureReserve(itemId);
    }

    public boolean shouldBackup() {
        return true;//TODO
    }

    public Operation getBackupOperation() {
        return new TxnPrepareBackupOperation(name, itemId, pollOperation);
    }

    protected void writeInternal(ObjectDataOutput out) throws IOException {
        super.writeInternal(out);
        out.writeLong(itemId);
        out.writeBoolean(pollOperation);
    }

    protected void readInternal(ObjectDataInput in) throws IOException {
        super.readInternal(in);
        itemId = in.readLong();
        pollOperation = in.readBoolean();
    }

    public int getId() {
        return QueueDataSerializerHook.TXN_PREPARE;
    }
}
