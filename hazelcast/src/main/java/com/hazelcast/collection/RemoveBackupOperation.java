package com.hazelcast.collection;

import com.hazelcast.collection.operation.CollectionOperation;
import com.hazelcast.spi.BackupOperation;

/**
 * @ali 8/31/13
 */
public class RemoveBackupOperation extends CollectionOperation implements BackupOperation {

    long itemId;

    public RemoveBackupOperation() {
        this.itemId = itemId;
    }

    public RemoveBackupOperation(String name, long itemId) {
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
