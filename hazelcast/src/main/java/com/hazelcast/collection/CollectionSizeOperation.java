package com.hazelcast.collection;

import com.hazelcast.collection.operation.CollectionOperation;

/**
 * @ali 8/31/13
 */
public class CollectionSizeOperation extends CollectionOperation {


    public void beforeRun() throws Exception {

    }

    public void run() throws Exception {

    }

    public void afterRun() throws Exception {

    }

    public int getId() {
        return CollectionDataSerializerHook.LIST_ADD;
    }
}
