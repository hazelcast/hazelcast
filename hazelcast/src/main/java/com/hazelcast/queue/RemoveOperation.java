package com.hazelcast.queue;

import com.hazelcast.nio.Data;
import com.hazelcast.spi.BackupOperation;
import com.hazelcast.spi.Operation;

/**
 * @ali 12/6/12
 */
public class RemoveOperation extends QueueDataOperation {

    public RemoveOperation() {
    }

    public RemoveOperation(String name, Data data) {
        super(name, data);
    }

    public void run() throws Exception {
        response = container.dataQueue.remove(data);
    }

    public Operation getBackupOperation() {
        return new QueueBackupOperation(new RemoveOperation());
    }
}
