package com.hazelcast.queue;

import com.hazelcast.spi.Operation;

/**
 * @ali 12/6/12
 */
public class PollOperation extends QueueKeyBasedOperation {

    public PollOperation() {
    }

    public PollOperation(String name) {
        super(name);
    }

    public void run() {
        response = container.dataQueue.poll();
    }

    public Operation getBackupOperation() {
        return new QueueBackupOperation(new PollOperation(name));
    }

}
