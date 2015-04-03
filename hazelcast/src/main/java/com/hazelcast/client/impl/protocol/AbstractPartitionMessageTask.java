package com.hazelcast.client.impl.protocol;

import com.hazelcast.instance.Node;
import com.hazelcast.nio.Connection;
import com.hazelcast.spi.Callback;
import com.hazelcast.spi.InvocationBuilder;
import com.hazelcast.spi.Operation;

/**
 * AbstractPartitionMessageTask
 */
public abstract class AbstractPartitionMessageTask<P>
        extends AbstractMessageTask<P> {

    private static final int TRY_COUNT = 100;

    protected AbstractPartitionMessageTask(ClientMessage clientMessage, Node node, Connection connection) {
        super(clientMessage, node, connection);
    }

    /**
     * Called on node side, before starting any operation.
     */
    protected void beforeProcess() {
    }

    /**
     * Called on node side, after process is run and right before sending the response to the client.
     */
    protected void beforeResponse() {
    }

    /**
     * Called on node side, after sending the response to the client.
     */
    protected void afterResponse() {
    }

    @Override
    public final void processMessage() {
        beforeProcess();
        Operation op = prepareOperation();
        op.setCallerUuid(endpoint.getUuid());
        InvocationBuilder builder = nodeEngine.getOperationService()
                      .createInvocationBuilder(getServiceName(), op, getPartitionId())
                      .setReplicaIndex(getReplicaIndex())
                      .setTryCount(TRY_COUNT)
                      .setResultDeserialized(false)
                      .setCallback(new CallbackImpl());
        builder.invoke();
    }

    public abstract String getServiceName();

    protected abstract Operation prepareOperation();

    protected int getReplicaIndex() {
        return 0;
    }

    protected byte[] filter(Object response) {
        //TODO handle binary response
        return null;
    }

    private class CallbackImpl
            implements Callback<Object> {

        public CallbackImpl() {
        }

        @Override
        public void notify(Object object) {
            beforeResponse();
            final byte[] result = filter(object);
            final ClientMessage resultParameters = GenericResultParameters.encode(result);
            sendClientMessage(resultParameters);
            afterResponse();
        }
    }
}
