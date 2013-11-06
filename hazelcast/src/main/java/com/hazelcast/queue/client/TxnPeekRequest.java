package com.hazelcast.queue.client;

import com.hazelcast.client.CallableClientRequest;
import com.hazelcast.client.ClientEndpoint;
import com.hazelcast.core.TransactionalQueue;
import com.hazelcast.nio.serialization.Portable;
import com.hazelcast.nio.serialization.PortableReader;
import com.hazelcast.nio.serialization.PortableWriter;
import com.hazelcast.queue.QueuePortableHook;
import com.hazelcast.queue.QueueService;
import com.hazelcast.transaction.TransactionContext;

import java.io.IOException;
import java.util.concurrent.TimeUnit;

/**
 * User: ahmetmircik
 * Date: 10/1/13
 * Time: 10:34 AM
 */
public class TxnPeekRequest extends CallableClientRequest implements Portable {

    private String name;

    private long timeout;

    public TxnPeekRequest(){
    }

    public TxnPeekRequest(String name, long timeout) {
        this.name = name;
        this.timeout = timeout;
    }

    @Override
    public Object call() throws Exception {
        final ClientEndpoint endpoint = getEndpoint();
        final TransactionContext context = endpoint.getTransactionContext();
        final TransactionalQueue queue = context.getQueue(name);
        return queue.peek(timeout, TimeUnit.MILLISECONDS);
    }

    @Override
    public String getServiceName() {
        return QueueService.SERVICE_NAME;
    }

    @Override
    public int getFactoryId() {
        return QueuePortableHook.F_ID;
    }

    @Override
    public int getClassId() {
        return QueuePortableHook.TXN_PEEK;
    }

    @Override
    public void writePortable(PortableWriter writer) throws IOException {
        writer.writeUTF("n",name);
        writer.writeLong("t",timeout);
    }

    @Override
    public void readPortable(PortableReader reader) throws IOException {
        name = reader.readUTF("n");
        timeout = reader.readLong("t");
    }
}
