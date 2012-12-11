package com.hazelcast.queue;

import com.hazelcast.spi.BackupAwareOperation;

/**
 * @ali 12/11/12
 */
public abstract class QueueBackupAwareOperation extends QueueOperation implements BackupAwareOperation{

    protected QueueBackupAwareOperation() {
    }

    protected QueueBackupAwareOperation(String name) {
        super(name);
    }

    public int getSyncBackupCount() {
        return getContainer().config.getSyncBackupCount();
    }

    public int getAsyncBackupCount() {
        return getContainer().config.getAsyncBackupCount();
    }



}
