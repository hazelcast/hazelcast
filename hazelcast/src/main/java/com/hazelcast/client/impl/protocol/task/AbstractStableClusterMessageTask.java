/*
 * Copyright (c) 2008-2019, Hazelcast, Inc. All Rights Reserved.
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
import com.hazelcast.core.ExecutionCallback;
import com.hazelcast.core.ICompletableFuture;
import com.hazelcast.instance.impl.Node;
import com.hazelcast.nio.Connection;
import com.hazelcast.spi.impl.operationservice.Operation;

import java.util.function.Supplier;

import static com.hazelcast.internal.util.InvocationUtil.invokeOnStableClusterSerial;

public abstract class AbstractStableClusterMessageTask<P> extends AbstractMessageTask<P> implements ExecutionCallback {

    private static final int RETRY_COUNT = 100;

    protected AbstractStableClusterMessageTask(ClientMessage clientMessage, Node node, Connection connection) {
        super(clientMessage, node, connection);
    }

    @Override
    protected void processMessage() throws Throwable {
        ICompletableFuture<Object> future = invokeOnStableClusterSerial(nodeEngine, createOperationSupplier(), RETRY_COUNT);
        future.andThen(this);
    }

    abstract Supplier<Operation> createOperationSupplier();

    protected abstract Object resolve(Object response);

    @Override
    public final void onResponse(Object response) {
        sendResponse(resolve(response));
    }

    @Override
    public final void onFailure(Throwable t) {
        handleProcessingFailure(t);
    }

}
