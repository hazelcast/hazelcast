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

import com.hazelcast.client.ClientEndpoint;
import com.hazelcast.client.impl.protocol.ClientMessage;
import com.hazelcast.client.impl.protocol.parameters.GenericResultParameters;
import com.hazelcast.core.ExecutionCallback;
import com.hazelcast.instance.Node;
import com.hazelcast.nio.Connection;
import com.hazelcast.nio.serialization.Data;
import com.hazelcast.spi.InternalCompletableFuture;
import com.hazelcast.spi.InvocationBuilder;
import com.hazelcast.spi.Operation;

public abstract class InvocationMessageTask<P> extends AbstractMessageTask<P> implements ExecutionCallback {

    private static final int TRY_COUNT = 100;

    protected InvocationMessageTask(ClientMessage clientMessage, Node node, Connection connection) {
        super(clientMessage, node, connection);
    }

    @Override
    protected void processMessage() {
        final ClientEndpoint endpoint = getEndpoint();
        Operation op = prepareOperation();
        op.setCallerUuid(endpoint.getUuid());

        InvocationBuilder builder = getInvocationBuilder(op)
                .setTryCount(TRY_COUNT)
                .setResultDeserialized(false);
        InternalCompletableFuture f = builder.invoke();
        f.andThen(this);
    }

    protected abstract InvocationBuilder getInvocationBuilder(Operation op);

    protected abstract Operation prepareOperation();

    protected ClientMessage encodeResponse(Object response) {
        final ClientMessage resultParameters;
        try {
            final Data responseData = (Data) response;
            resultParameters = GenericResultParameters.encode(responseData);
            return resultParameters;
        } catch (ClassCastException e) {
            logger.severe("Unsupported response type :" + response.getClass().getName());
            throw e;
        }
    }

    @Override
    public void onResponse(Object response) {
        final ClientMessage resultParameters = encodeResponse(response);
        sendClientMessage(resultParameters);
    }

    @Override
    public void onFailure(Throwable t) {
        sendClientMessage(t);
    }
}
