package com.hazelcast.collection;

import com.hazelcast.core.ItemEventType;
import com.hazelcast.nio.serialization.Data;
import com.hazelcast.spi.Operation;

import java.util.Map;

/**
 * @ali 8/31/13
 */
public class CollectionClearOperation extends CollectionBackupAwareOperation {

    private transient Map<Long, Data> itemIdMap;


    public CollectionClearOperation() {
    }

    public CollectionClearOperation(String name) {
        super(name);
    }

    public boolean shouldBackup() {
        return itemIdMap != null && !itemIdMap.isEmpty();
    }

    public Operation getBackupOperation() {
        return new CollectionClearBackupOperation(name, itemIdMap.keySet());
    }

    public int getId() {
        return CollectionDataSerializerHook.COLLECTION_CLEAR;
    }

    public void beforeRun() throws Exception {

    }

    public void run() throws Exception {
        itemIdMap = getOrCreateContainer().clear();
    }

    public void afterRun() throws Exception {
        for (Data value : itemIdMap.values()) {
            publishEvent(ItemEventType.REMOVED, value);
        }
    }
}
