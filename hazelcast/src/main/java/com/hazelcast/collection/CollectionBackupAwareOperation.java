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
        //TODO getFrom container config
        return 0;
    }

    public int getAsyncBackupCount() {
        //TODO getFrom container config
        return 0;
    }
}
