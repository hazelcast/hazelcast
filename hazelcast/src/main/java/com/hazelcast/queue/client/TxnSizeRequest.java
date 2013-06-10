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

/**
 * @ali 6/7/13
 */
public class TxnSizeRequest extends CallableClientRequest implements Portable {

    String name;

    public TxnSizeRequest() {
    }

    public TxnSizeRequest(String name) {
        this.name = name;
    }

    public Object call() throws Exception {
        final ClientEndpoint endpoint = getEndpoint();
        final TransactionContext context = endpoint.getTransactionContext();
        final TransactionalQueue queue = context.getQueue(name);
        return queue.size();
    }

    public String getServiceName() {
        return QueueService.SERVICE_NAME;
    }

    public int getFactoryId() {
        return QueuePortableHook.F_ID;
    }

    public int getClassId() {
        return QueuePortableHook.TXN_SIZE;
    }

    public void writePortable(PortableWriter writer) throws IOException {
        writer.writeUTF("n",name);
    }

    public void readPortable(PortableReader reader) throws IOException {
        name = reader.readUTF("n");
    }
}
