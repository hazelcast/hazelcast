package com.hazelcast.atomicNumber;

import com.hazelcast.spi.BackupAwareOperation;
import com.hazelcast.spi.KeyBasedOperation;

// author: sancar - 25.12.2012
public abstract class AtomicNumberBackupAwareOperation extends  AtomicNumberBaseOperation implements BackupAwareOperation, KeyBasedOperation {

    protected boolean shouldBackup = true;

    public AtomicNumberBackupAwareOperation(){
        super();
    }

    public AtomicNumberBackupAwareOperation(String name){
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

    public int getKeyHash() {
        String key = AtomicNumberService.NAME + getName();
        return key.hashCode();
    }
}
