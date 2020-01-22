/*
 * Copyright (c) 2008-2020, Hazelcast, Inc. All Rights Reserved.
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

import com.hazelcast.client.impl.operations.OperationFactoryWrapper;
import com.hazelcast.client.impl.protocol.ClientMessage;
import com.hazelcast.instance.impl.Node;
import com.hazelcast.internal.nio.Connection;
import com.hazelcast.spi.impl.operationservice.OperationFactory;
import com.hazelcast.spi.impl.operationservice.impl.OperationServiceImpl;

import java.util.Map;
import java.util.concurrent.CompletableFuture;

public abstract class AbstractAllPartitionsMessageTask<P>
        extends AbstractAsyncMessageTask<P, Map<Integer, Object>> {

    public AbstractAllPartitionsMessageTask(ClientMessage clientMessage, Node node, Connection connection) {
        super(clientMessage, node, connection);
    }

    @Override
    protected CompletableFuture<Map<Integer, Object>> processInternal() {
        OperationFactory operationFactory = new OperationFactoryWrapper(createOperationFactory(), endpoint.getUuid());
        OperationServiceImpl operationService = nodeEngine.getOperationService();
        return operationService.invokeOnAllPartitionsAsync(getServiceName(), operationFactory);
    }

    @Override
    protected Object processResponseBeforeSending(Map<Integer, Object> response) {
        return reduce(response);
    }

    protected abstract OperationFactory createOperationFactory();

    protected abstract Object reduce(Map<Integer, Object> map);

}
