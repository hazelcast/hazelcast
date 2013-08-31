package com.hazelcast.collection;

import com.hazelcast.collection.operation.CollectionOperation;
import com.hazelcast.spi.BackupOperation;

/**
 * @ali 8/31/13
 */
public class CollectionRemoveBackupOperation extends CollectionOperation implements BackupOperation {

    long itemId;

    public CollectionRemoveBackupOperation() {
        this.itemId = itemId;
    }

    public CollectionRemoveBackupOperation(String name, long itemId) {
        super(name);
        this.itemId = itemId;
    }

    public void beforeRun() throws Exception {

    }

    public void run() throws Exception {

    }

    public void afterRun() throws Exception {

    }

    public int getId() {
        return 0;
    }
}
