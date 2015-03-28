package com.hazelcast.spi.impl.operationservice.impl;

import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.spi.AbstractOperation;
import com.hazelcast.spi.BackupAwareOperation;
import com.hazelcast.spi.Operation;

import java.io.IOException;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

class DummyBackupAwareOperation extends AbstractOperation implements BackupAwareOperation {

    public final static ConcurrentMap<String, Integer> backupCompletedMap = new ConcurrentHashMap<String, Integer>();

    public int syncBackupCount;
    public int asyncBackupCount;
    public boolean returnsResponse = true;
    public String backupKey;

    public DummyBackupAwareOperation() {
    }

    public DummyBackupAwareOperation(int partitionId) {
        setPartitionId(partitionId);
    }

    @Override
    public boolean returnsResponse() {
        return returnsResponse;
    }

    @Override
    public boolean shouldBackup() {
        return true;
    }

    @Override
    public int getSyncBackupCount() {
        return syncBackupCount;
    }

    @Override
    public int getAsyncBackupCount() {
        return asyncBackupCount;
    }

    @Override
    public Operation getBackupOperation() {
        System.out.println("DummyBackupOperation created");
        return new DummyBackupOperation(backupKey);
    }

    @Override
    public void run() throws Exception {
        System.out.println("DummyBackupAwareOperation completed");
        if (backupKey != null) {
            backupCompletedMap.put(backupKey, 0);
        }
    }

    @Override
    protected void writeInternal(ObjectDataOutput out) throws IOException {
        super.writeInternal(out);
        out.writeInt(syncBackupCount);
        out.writeInt(asyncBackupCount);
        out.writeBoolean(returnsResponse);
        out.writeUTF(backupKey);
    }

    @Override
    protected void readInternal(ObjectDataInput in) throws IOException {
        super.readInternal(in);
        syncBackupCount = in.readInt();
        asyncBackupCount = in.readInt();
        returnsResponse = in.readBoolean();
        backupKey = in.readUTF();
    }
}