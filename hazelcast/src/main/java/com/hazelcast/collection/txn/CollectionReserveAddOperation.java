package com.hazelcast.collection.txn;

import com.hazelcast.collection.CollectionDataSerializerHook;
import com.hazelcast.collection.CollectionOperation;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;

import java.io.IOException;

/**
 * @ali 9/3/13
 */
public class CollectionReserveAddOperation extends CollectionOperation {

    String transactionId;

    public CollectionReserveAddOperation() {
    }

    public CollectionReserveAddOperation(String name, String transactionId) {
        super(name);
        this.transactionId = transactionId;
    }

    public int getId() {
        return CollectionDataSerializerHook.COLLECTION_RESERVE_ADD;
    }

    public void beforeRun() throws Exception {

    }

    public void run() throws Exception {
        response = getOrCreateContainer().reserveAdd(transactionId);
    }

    public void afterRun() throws Exception {

    }

    protected void writeInternal(ObjectDataOutput out) throws IOException {
        super.writeInternal(out);
        out.writeUTF(transactionId);
    }

    protected void readInternal(ObjectDataInput in) throws IOException {
        super.readInternal(in);
        transactionId = in.readUTF();
    }
}
