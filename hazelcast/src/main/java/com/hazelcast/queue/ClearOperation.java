package com.hazelcast.queue;

import com.hazelcast.spi.BackupAwareOperation;
import com.hazelcast.spi.BackupOperation;
import com.hazelcast.spi.KeyBasedOperation;
import com.hazelcast.spi.Operation;

/**
 * @ali 12/6/12
 */
public class ClearOperation extends QueueBackupAwareOperation {

    public ClearOperation() {
    }

    public ClearOperation(String name) {
        super(name);
    }

    public void run() throws Exception {
        getContainer().dataQueue.clear();
        response = true;
    }

    public Operation getBackupOperation() {
        return new ClearBackupOperation(name);
    }
}
