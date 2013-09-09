package com.hazelcast.collection;

import com.hazelcast.spi.BackupAwareOperation;

/**
 * @ali 8/30/13
 */
public abstract class CollectionBackupAwareOperation extends CollectionOperation implements BackupAwareOperation {

    protected CollectionBackupAwareOperation() {
    }

    protected CollectionBackupAwareOperation(String name) {
        super(name);
    }

    public int getSyncBackupCount() {
        //TODO getFrom collection config
        return 1;
    }

    public int getAsyncBackupCount() {
        //TODO getFrom collection config
        return 0;
    }
}
