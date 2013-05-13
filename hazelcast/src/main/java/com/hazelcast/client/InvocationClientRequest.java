package com.hazelcast.client;

import com.hazelcast.nio.Address;
import com.hazelcast.spi.InvocationBuilder;
import com.hazelcast.spi.Operation;

/**
 * @mdogan 5/13/13
 */
public abstract class InvocationClientRequest extends ClientRequest {

    final void process() throws Exception {
        invoke();
    }

    protected abstract void invoke();

    protected final InvocationBuilder createInvocationBuilder(String serviceName, Operation op, final int partitionId) {
        return clientEngine.createInvocationBuilder(serviceName, op, partitionId);
    }

    protected final InvocationBuilder createInvocationBuilder(String serviceName, Operation op, Address target) {
        return clientEngine.createInvocationBuilder(serviceName, op, target);
    }

}
