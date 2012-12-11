package com.hazelcast.queue;

import com.hazelcast.spi.BackupOperation;

/**
 * @ali 12/11/12
 */
public class PollBackupOperation extends QueueOperation implements BackupOperation {

    public PollBackupOperation() {
    }

    public PollBackupOperation(String name) {
        super(name);
    }

    public void run() throws Exception {
        getContainer().dataQueue.poll();
        response = Boolean.TRUE;
    }
}
