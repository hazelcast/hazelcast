package com.hazelcast.queue;

import com.hazelcast.nio.Data;
import com.hazelcast.spi.BackupAwareOperation;
import com.hazelcast.spi.Notifier;
import com.hazelcast.spi.Operation;
import com.hazelcast.spi.WaitSupport;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * User: ali
 * Date: 11/14/12
 * Time: 12:14 AM
 */
public class OfferOperation extends QueueBackupAwareOperation implements WaitSupport, Notifier {

    private Data data;

    public OfferOperation() {
    }

    public OfferOperation(final String name, final Data data) {
        super(name);
        this.data = data;
    }

    public void run() {
        response = getContainer().dataQueue.offer(data);
    }

    public Operation getBackupOperation() {
        return new OfferBackupOperation(name, data);
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

    public void writeInternal(DataOutput out) throws IOException {
        super.writeInternal(out);
        data.writeData(out);
    }

    public void readInternal(DataInput in) throws IOException {
        super.readInternal(in);
        data = new Data();
        data.readData(in);
    }
}
