package com.hazelcast.queue.tx;

import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.queue.QueueDataSerializerHook;
import com.hazelcast.queue.QueueOperation;

import java.io.IOException;

/**
 * User: ahmetmircik
 * Date: 9/30/13
 * Time: 2:19 PM
 */
public class TxnPeekOperation extends QueueOperation {

    long itemId;

    String transactionId;

    public TxnPeekOperation() {

    }

    public TxnPeekOperation(String name, long timeoutMillis, long itemId, String transactionId) {
        super(name, timeoutMillis);
        this.itemId = itemId;
        this.transactionId = transactionId;
    }


    @Override
    public void run() throws Exception {
        response = getOrCreateContainer().txnPeek(itemId, transactionId);
    }


    @Override
    public void afterRun() throws Exception {
        if (response != null) {
            getQueueService().getLocalQueueStatsImpl(name).incrementOtherOperations();
        }
    }

    @Override
    public int getId() {
        return QueueDataSerializerHook.TXN_PEEK;
    }

    protected void writeInternal(ObjectDataOutput out) throws IOException {
        super.writeInternal(out);
        out.writeLong(itemId);
        out.writeUTF(transactionId);
    }

    protected void readInternal(ObjectDataInput in) throws IOException {
        super.readInternal(in);
        itemId = in.readLong();
        transactionId = in.readUTF();
    }
}
