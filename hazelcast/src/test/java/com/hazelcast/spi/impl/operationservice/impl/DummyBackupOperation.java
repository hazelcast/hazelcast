package com.hazelcast.spi.impl.operationservice.impl;

import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.spi.AbstractOperation;
import com.hazelcast.spi.BackupOperation;

import java.io.IOException;
import java.util.concurrent.ConcurrentMap;

class DummyBackupOperation extends AbstractOperation implements BackupOperation {

    private String backupKey;

    public DummyBackupOperation() {
    }

    public DummyBackupOperation(String  backupKey) {
        this.backupKey = backupKey;
    }

    @Override
    public void run() throws Exception {
        System.out.println("DummyBackupOperation completed");

        if (backupKey == null) {
            return;
        }

        ConcurrentMap<String, Integer> backupCompletedMap = DummyBackupAwareOperation.backupCompletedMap;
        for (; ; ) {
            Integer prev = backupCompletedMap.get(backupKey);
            if (prev == null) {
                Integer found = backupCompletedMap.putIfAbsent(backupKey, 1);
                if (found == null) {
                    return;
                }

                prev = found;
            }

            Integer next = prev + 1;
            if (backupCompletedMap.replace(backupKey, prev, next)) {
                return;
            }
        }
    }

    @Override
    protected void writeInternal(ObjectDataOutput out) throws IOException {
        super.writeInternal(out);
        out.writeUTF(backupKey);
    }

    @Override
    protected void readInternal(ObjectDataInput in) throws IOException {
        super.readInternal(in);
        backupKey = in.readUTF();
    }
}
