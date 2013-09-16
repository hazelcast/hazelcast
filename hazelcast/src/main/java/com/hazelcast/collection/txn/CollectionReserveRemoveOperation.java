package com.hazelcast.collection.txn;

import com.hazelcast.collection.CollectionDataSerializerHook;
import com.hazelcast.collection.CollectionOperation;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.Data;

import java.io.IOException;

/**
 * @ali 9/3/13
 */
public class CollectionReserveRemoveOperation extends CollectionOperation {

    private long reservedItemId = -1;

    private Data value;

    String transactionId;

    public CollectionReserveRemoveOperation() {
    }

    public CollectionReserveRemoveOperation(String name, long reservedItemId, Data value, String transactionId) {
        super(name);
        this.reservedItemId = reservedItemId;
        this.value = value;
        this.transactionId = transactionId;
    }

    public int getId() {
        return CollectionDataSerializerHook.COLLECTION_RESERVE_REMOVE;
    }

    public void beforeRun() throws Exception {

    }

    public void run() throws Exception {
        response = getOrCreateContainer().reserveRemove(reservedItemId, value, transactionId);
    }

    public void afterRun() throws Exception {

    }

    protected void writeInternal(ObjectDataOutput out) throws IOException {
        super.writeInternal(out);
        out.writeLong(reservedItemId);
        value.writeData(out);
        out.writeUTF(transactionId);
    }

    protected void readInternal(ObjectDataInput in) throws IOException {
        super.readInternal(in);
        reservedItemId = in.readLong();
        value = new Data();
        value.readData(in);
        transactionId = in.readUTF();
    }
}
