package com.hazelcast.collection.txn;

import com.hazelcast.collection.CollectionDataSerializerHook;
import com.hazelcast.collection.CollectionOperation;

/**
 * @ali 9/3/13
 */
public class CollectionReserveAddOperation extends CollectionOperation {

    public CollectionReserveAddOperation() {
    }

    public CollectionReserveAddOperation(String name) {
        super(name);
    }

    public int getId() {
        return CollectionDataSerializerHook.COLLECTION_RESERVE_ADD;
    }

    public void beforeRun() throws Exception {

    }

    public void run() throws Exception {
        response = getOrCreateContainer().reserveAdd();
    }

    public void afterRun() throws Exception {

    }
}
