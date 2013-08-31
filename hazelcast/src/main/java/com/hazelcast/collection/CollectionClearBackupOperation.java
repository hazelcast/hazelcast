package com.hazelcast.collection;

import com.hazelcast.collection.operation.CollectionOperation;
import com.hazelcast.spi.BackupOperation;

import java.util.Set;

/**
 * @ali 8/31/13
 */
public class CollectionClearBackupOperation extends CollectionOperation implements BackupOperation {
    
    Set<Long> itemIdSet;

    public CollectionClearBackupOperation() {
    }

    public CollectionClearBackupOperation(String name, Set<Long> itemIdSet) {
        super(name);
        this.itemIdSet = itemIdSet;
    }

    public int getId() {
        return CollectionDataSerializerHook.COLLECTION_CLEAR_BACKUP;
    }

    public void beforeRun() throws Exception {

    }

    public void run() throws Exception {
        getOrCreateContainer().clearBackup(itemIdSet);
    }

    public void afterRun() throws Exception {

    }
}
