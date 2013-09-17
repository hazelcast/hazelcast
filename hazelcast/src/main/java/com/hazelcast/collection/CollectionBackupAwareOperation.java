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
        return getOrCreateContainer().getConfig().getBackupCount();
    }

    public int getAsyncBackupCount() {
        return getOrCreateContainer().getConfig().getAsyncBackupCount();
    }
}
