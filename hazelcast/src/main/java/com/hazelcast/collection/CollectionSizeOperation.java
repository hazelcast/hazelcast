package com.hazelcast.collection;

import com.hazelcast.collection.operation.CollectionOperation;

/**
 * @ali 8/31/13
 */
public class CollectionSizeOperation extends CollectionOperation {

    public CollectionSizeOperation() {
    }

    public CollectionSizeOperation(String name) {
        super(name);
    }

    public void beforeRun() throws Exception {

    }

    public void run() throws Exception {
        response = getOrCreateContainer().size();
    }

    public void afterRun() throws Exception {

    }

    public int getId() {
        return CollectionDataSerializerHook.COLLECTION_SIZE;
    }
}
