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

package com.hazelcast.client.impl.protocol.task.executorservice;

import com.hazelcast.client.impl.protocol.ClientMessage;
import com.hazelcast.client.impl.protocol.codec.ExecutorServiceCancelOnAddressCodec;
import com.hazelcast.client.impl.protocol.task.AbstractAddressMessageTask;
import com.hazelcast.executor.impl.DistributedExecutorService;
import com.hazelcast.executor.impl.operations.CancellationOperation;
import com.hazelcast.instance.impl.Node;
import com.hazelcast.nio.Address;
import com.hazelcast.nio.Connection;
import com.hazelcast.spi.impl.operationservice.Operation;

import java.security.Permission;

public class ExecutorServiceCancelOnAddressMessageTask
        extends AbstractAddressMessageTask<ExecutorServiceCancelOnAddressCodec.RequestParameters> {

    public ExecutorServiceCancelOnAddressMessageTask(ClientMessage clientMessage, Node node, Connection connection) {
        super(clientMessage, node, connection);
    }

    @Override
    protected Operation prepareOperation() {
        return new CancellationOperation(parameters.uuid, parameters.interrupt);
    }

    @Override
    protected Address getAddress() {
        return parameters.address;
    }

    @Override
    protected ExecutorServiceCancelOnAddressCodec.RequestParameters decodeClientMessage(ClientMessage clientMessage) {
        parameters = ExecutorServiceCancelOnAddressCodec.decodeRequest(clientMessage);
        parameters.address = clientEngine.memberAddressOf(parameters.address);
        return parameters;
    }

    @Override
    protected ClientMessage encodeResponse(Object response) {
        return ExecutorServiceCancelOnAddressCodec.encodeResponse((Boolean) response);
    }

    @Override
    public String getDistributedObjectName() {
        return null;
    }

    @Override
    public String getServiceName() {
        return DistributedExecutorService.SERVICE_NAME;
    }

    @Override
    public Permission getRequiredPermission() {
        return null;
    }

    @Override
    public String getMethodName() {
        return "cancel";
    }

    @Override
    public Object[] getParameters() {
        return null;
    }
}
