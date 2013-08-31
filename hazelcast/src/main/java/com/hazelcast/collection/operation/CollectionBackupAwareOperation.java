package com.hazelcast.collection.operation;

import com.hazelcast.collection.list.ListService;
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

    public boolean shouldBackup() {
        return false;
    }

    public int getSyncBackupCount() {
        //TODO getFrom container config
        return 0;
    }

    public int getAsyncBackupCount() {
        //TODO getFrom container config
        return 0;
    }

    public final String getServiceName() {
        return ListService.SERVICE_NAME;
    }

}
