/*
 * Copyright (c) 2008-2016, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.client.impl.protocol.task.executorservice;

import com.hazelcast.client.impl.protocol.ClientMessage;
import com.hazelcast.client.impl.protocol.codec.ExecutorServiceCancelOnAddressCodec;
import com.hazelcast.executor.impl.DistributedExecutorService;
import com.hazelcast.executor.impl.operations.CancellationOperation;
import com.hazelcast.instance.Node;
import com.hazelcast.nio.Connection;
import com.hazelcast.spi.InvocationBuilder;
import com.hazelcast.spi.impl.operationservice.InternalOperationService;

import java.net.UnknownHostException;

public class ExecutorServiceCancelOnAddressMessageTask
        extends AbstractExecutorServiceCancelMessageTask<ExecutorServiceCancelOnAddressCodec.RequestParameters> {

    public ExecutorServiceCancelOnAddressMessageTask(ClientMessage clientMessage, Node node, Connection connection) {
        super(clientMessage, node, connection);
    }

    @Override
    protected InvocationBuilder createInvocationBuilder() throws UnknownHostException {
        final InternalOperationService operationService = nodeEngine.getOperationService();
        final String serviceName = DistributedExecutorService.SERVICE_NAME;
        CancellationOperation op = new CancellationOperation(parameters.uuid, parameters.interrupt);
        return operationService.createInvocationBuilder(serviceName, op, parameters.address);
    }


    @Override
    protected ExecutorServiceCancelOnAddressCodec.RequestParameters decodeClientMessage(ClientMessage clientMessage) {
        return ExecutorServiceCancelOnAddressCodec.decodeRequest(clientMessage);
    }

    @Override
    protected ClientMessage encodeResponse(Object response) {
        return ExecutorServiceCancelOnAddressCodec.encodeResponse((Boolean) response);
    }

    @Override
    public String getDistributedObjectName() {
        return null;
    }

}
