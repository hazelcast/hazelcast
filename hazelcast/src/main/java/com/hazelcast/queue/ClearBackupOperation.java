package com.hazelcast.queue;

import com.hazelcast.spi.BackupOperation;

/**
 * @ali 12/11/12
 */
public class ClearBackupOperation extends QueueOperation implements BackupOperation {

    public ClearBackupOperation() {
    }

    public ClearBackupOperation(String name) {
        super(name);
    }

    public void run() throws Exception {
        getContainer().dataQueue.clear();
        response = true;
    }
}
