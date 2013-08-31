package com.hazelcast.collection;

import com.hazelcast.collection.operation.CollectionBackupAwareOperation;
import com.hazelcast.spi.Operation;

import java.util.Set;

/**
 * @ali 8/31/13
 */
public class CollectionClearOperation extends CollectionBackupAwareOperation {

    private transient Set<Long> itemIdSet;


    public CollectionClearOperation() {
    }

    public CollectionClearOperation(String name) {
        super(name);
    }

    public boolean shouldBackup() {
        return itemIdSet != null && !itemIdSet.isEmpty();
    }

    public Operation getBackupOperation() {
        return new CollectionClearBackupOperation(name, itemIdSet);
    }

    public int getId() {
        return CollectionDataSerializerHook.COLLECTION_CLEAR;
    }

    public void beforeRun() throws Exception {

    }

    public void run() throws Exception {
        itemIdSet = getOrCreateContainer().clear();
    }

    public void afterRun() throws Exception {

    }
}
