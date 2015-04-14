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
import com.hazelcast.client.impl.operations.OperationFactoryWrapper;
import com.hazelcast.client.impl.protocol.ClientMessage;
import com.hazelcast.instance.Node;
import com.hazelcast.nio.Connection;
import com.hazelcast.spi.OperationFactory;
import com.hazelcast.spi.impl.operationservice.InternalOperationService;

import java.util.Collection;
import java.util.Map;

public abstract class AbstractMultiPartitionMessageTask<P> extends AbstractCallableMessageTask<P> {

    protected AbstractMultiPartitionMessageTask(ClientMessage clientMessage, Node node, Connection connection) {
        super(clientMessage, node, connection);
    }

    @Override
    protected ClientMessage call() throws Exception {
        ClientEndpoint endpoint = getEndpoint();
        OperationFactory operationFactory = new OperationFactoryWrapper(createOperationFactory(), endpoint.getUuid());

        final InternalOperationService operationService = nodeEngine.getOperationService();
        Map<Integer, Object> map = operationService.invokeOnPartitions(getServiceName(), operationFactory, getPartitions());
        return reduce(map);
    }


    protected abstract OperationFactory createOperationFactory();

    protected abstract ClientMessage reduce(Map<Integer, Object> map);

    public abstract Collection<Integer> getPartitions();

}
