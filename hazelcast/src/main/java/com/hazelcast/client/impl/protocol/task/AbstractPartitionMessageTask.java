/*
 * Copyright (c) 2008-2015, Hazelcast, Inc. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.hazelcast.client.impl.protocol.task;

import com.hazelcast.client.impl.protocol.ClientMessage;
import com.hazelcast.client.impl.protocol.parameters.GenericResultParameters;
import com.hazelcast.instance.Node;
import com.hazelcast.nio.Connection;
import com.hazelcast.nio.serialization.Data;
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

    protected abstract Operation prepareOperation();

    protected int getReplicaIndex() {
        return 0;
    }

    protected byte[] filter(Object response) {
        if (response == null) {
            return null;
        }
        Data data = (Data) response;
        return data.toByteArray();
    }

    private class CallbackImpl implements Callback<Object> {

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
