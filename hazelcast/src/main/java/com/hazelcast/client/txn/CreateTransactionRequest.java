package com.hazelcast.client.txn;

import com.hazelcast.client.CallableClientRequest;
import com.hazelcast.client.ClientEndpoint;
import com.hazelcast.client.ClientEngineImpl;
import com.hazelcast.nio.serialization.Portable;
import com.hazelcast.nio.serialization.PortableReader;
import com.hazelcast.nio.serialization.PortableWriter;
import com.hazelcast.transaction.TransactionContext;
import com.hazelcast.transaction.TransactionManagerService;
import com.hazelcast.transaction.TransactionOptions;

import java.io.IOException;

/**
 * @ali 6/6/13
 */
public class CreateTransactionRequest extends CallableClientRequest implements Portable {

    TransactionOptions options;

    public CreateTransactionRequest() {
    }

    public CreateTransactionRequest(TransactionOptions options) {
        this.options = options;
    }

    public Object call() throws Exception {
        ClientEngineImpl clientEngine = getService();
        final ClientEndpoint endpoint = getEndpoint();
        final TransactionManagerService transactionManagerService = clientEngine.getTransactionManagerService();
        final TransactionContext context = transactionManagerService.newTransactionContext(options);
        endpoint.setTransactionContext(context);
        return context.getTxnId();
    }

    public String getServiceName() {
        return ClientEngineImpl.SERVICE_NAME;
    }

    public int getFactoryId() {
        return ClientTxnPortableHook.F_ID;
    }

    public int getClassId() {
        return ClientTxnPortableHook.CREATE;
    }

    public void writePortable(PortableWriter writer) throws IOException {
        options.writeData(writer.getRawDataOutput());
    }

    public void readPortable(PortableReader reader) throws IOException {
        options = new TransactionOptions();
        options.readData(reader.getRawDataInput());
    }
}
