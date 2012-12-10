package com.hazelcast.queue;

import com.hazelcast.nio.Data;
import com.hazelcast.spi.Notifier;
import com.hazelcast.spi.Operation;
import com.hazelcast.spi.WaitSupport;

/**
 * User: ali
 * Date: 11/14/12
 * Time: 12:14 AM
 */
public class OfferOperation extends QueueDataOperation implements WaitSupport, Notifier {

    public OfferOperation() {
    }

    public OfferOperation(final String name, final Data data) {
        super(name, data);
    }

    public void run() {
        response = container.dataQueue.offer(data);
    }

    public Operation getBackupOperation() {
        return new QueueBackupOperation(new OfferOperation(name, data));
    }

    public Object getNotifiedKey() {
        return getName() + ":take";
    }

    public Object getWaitKey() {
        return getName() + ":offer";
    }

    public boolean shouldWait() {
//        return container.dataQueue.size() >= Queue.MaxSize;
        return false;
    }

    public long getWaitTimeoutMillis() {
        return 0;
    }

    public void onWaitExpire() {
        getResponseHandler().sendResponse(Boolean.FALSE);
    }
}
