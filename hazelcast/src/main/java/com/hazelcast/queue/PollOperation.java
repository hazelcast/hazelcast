package com.hazelcast.queue;

import com.hazelcast.spi.Notifier;
import com.hazelcast.spi.Operation;
import com.hazelcast.spi.WaitSupport;

/**
 * @ali 12/6/12
 */
public class PollOperation extends QueueTimedOperation implements WaitSupport, Notifier {

    public PollOperation() {
    }

    public PollOperation(String name, long timeoutMillis) {
        super(name, timeoutMillis);
    }

    public void run() {
        response = getContainer().dataQueue.poll();
    }

    public Operation getBackupOperation() {
        return new PollBackupOperation(name);
    }

    public Object getNotifiedKey() {
        return getName() + ":offer";
    }

    public Object getWaitKey() {
        return getName() + ":take";
    }

    public boolean shouldWait() {
        return getWaitTimeoutMillis() != 0 && getContainer().dataQueue.size() == 0;
    }

    public void onWaitExpire() {
        getResponseHandler().sendResponse(null);
    }
}
