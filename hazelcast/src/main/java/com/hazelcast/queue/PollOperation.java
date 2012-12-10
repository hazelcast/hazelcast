package com.hazelcast.queue;

import com.hazelcast.spi.Notifier;
import com.hazelcast.spi.Operation;
import com.hazelcast.spi.WaitSupport;

/**
 * @ali 12/6/12
 */
public class PollOperation extends TimedQueueOperation implements WaitSupport, Notifier {

    public PollOperation() {
    }

    public PollOperation(String name, long timeoutMillis) {
        super(name, timeoutMillis);
    }

    public void run() {
        response = container.dataQueue.poll();
    }

    public Operation getBackupOperation() {
        return new QueueBackupOperation(new PollOperation(name, 0));
    }

    public Object getNotifiedKey() {
        return getName() + ":offer";
    }

    public Object getWaitKey() {
        return getName() + ":take";
    }

    public boolean shouldWait() {
        return getTimeoutMillis() > 0 && container.dataQueue.size() == 0;
    }

    public long getWaitTimeoutMillis() {
        return getTimeoutMillis();
    }

    public void onWaitExpire() {
        getResponseHandler().sendResponse(null);
    }
}
