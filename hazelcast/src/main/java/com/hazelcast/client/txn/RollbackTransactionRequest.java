package com.hazelcast.client.txn;

import com.hazelcast.client.CallableClientRequest;
import com.hazelcast.client.ClientEndpoint;
import com.hazelcast.client.ClientEngineImpl;
import com.hazelcast.nio.serialization.Portable;
import com.hazelcast.nio.serialization.PortableReader;
import com.hazelcast.nio.serialization.PortableWriter;

import java.io.IOException;

/**
 * @author ali 6/7/13
 */
public class RollbackTransactionRequest extends CallableClientRequest implements Portable {

    public RollbackTransactionRequest() {
    }

    public Object call() throws Exception {
        final ClientEndpoint endpoint = getEndpoint();
        endpoint.getTransactionContext().rollbackTransaction();
        return null;
    }

    public String getServiceName() {
        return ClientEngineImpl.SERVICE_NAME;
    }

    public int getFactoryId() {
        return ClientTxnPortableHook.F_ID;
    }

    public int getClassId() {
        return ClientTxnPortableHook.ROLLBACK;
    }

    public void writePortable(PortableWriter writer) throws IOException {
    }

    public void readPortable(PortableReader reader) throws IOException {
    }
}
