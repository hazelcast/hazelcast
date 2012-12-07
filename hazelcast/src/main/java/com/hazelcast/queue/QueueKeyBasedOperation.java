package com.hazelcast.queue;

import com.hazelcast.spi.BackupAwareOperation;
import com.hazelcast.spi.KeyBasedOperation;

/**
 * @ali 12/6/12
 */
public abstract class QueueKeyBasedOperation extends QueueOperation implements KeyBasedOperation, BackupAwareOperation {

    protected QueueKeyBasedOperation() {
    }

    protected QueueKeyBasedOperation(String name) {
        super(name);
    }

    public int getKeyHash() {
        return name.hashCode();
    }

    public int getSyncBackupCount() {
        return container.config.getSyncBackupCount();
    }

    public int getAsyncBackupCount() {
        return container.config.getAsyncBackupCount();
    }
}
