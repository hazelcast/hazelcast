package com.hazelcast.concurrent.atomicreference;

import com.hazelcast.spi.BackupAwareOperation;

public abstract class AtomicReferenceBackupAwareOperation extends AtomicReferenceBaseOperation implements BackupAwareOperation {

    protected boolean shouldBackup = true;

    public AtomicReferenceBackupAwareOperation() {
        super();
    }

    public AtomicReferenceBackupAwareOperation(String name) {
        super(name);
    }

    public boolean shouldBackup() {
        return shouldBackup;
    }

    public int getSyncBackupCount() {
        return 1;
    }

    public int getAsyncBackupCount() {
        return 0;
    }

}
