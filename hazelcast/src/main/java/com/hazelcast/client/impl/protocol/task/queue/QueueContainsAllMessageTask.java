/*
 * Copyright (c) 2008-2023, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.client.impl.protocol.task.queue;

import com.hazelcast.client.impl.protocol.ClientMessage;
import com.hazelcast.client.impl.protocol.codec.QueueContainsAllCodec;
import com.hazelcast.collection.impl.queue.operations.ContainsOperation;
import com.hazelcast.instance.impl.Node;
import com.hazelcast.internal.nio.Connection;
import com.hazelcast.security.SecurityInterceptorConstants;
import com.hazelcast.security.permission.ActionConstants;
import com.hazelcast.security.permission.QueuePermission;
import com.hazelcast.spi.impl.operationservice.Operation;

import java.security.Permission;

/**
 * Client Protocol Task for handling messages with type ID:
 * {@link com.hazelcast.client.impl.protocol.codec.QueueMessageType#QUEUE_CONTAINSALL}
 */
public class QueueContainsAllMessageTask
        extends AbstractQueueMessageTask<QueueContainsAllCodec.RequestParameters> {

    public QueueContainsAllMessageTask(ClientMessage clientMessage, Node node, Connection connection) {
        super(clientMessage, node, connection);
    }

    @Override
    protected Operation prepareOperation() {
        return new ContainsOperation(parameters.name, parameters.dataList);
    }

    @Override
    protected QueueContainsAllCodec.RequestParameters decodeClientMessage(ClientMessage clientMessage) {
        return QueueContainsAllCodec.decodeRequest(clientMessage);
    }

    @Override
    protected ClientMessage encodeResponse(Object response) {
        final boolean result = response != null && ((Boolean) response);
        return QueueContainsAllCodec.encodeResponse(result);
    }

    @Override
    public Object[] getParameters() {
        return new Object[]{parameters.dataList};
    }

    @Override
    public Permission getRequiredPermission() {
        return new QueuePermission(parameters.name, ActionConstants.ACTION_READ);
    }

    @Override
    public String getMethodName() {
        return SecurityInterceptorConstants.CONTAINS_ALL;
    }

    @Override
    public String getDistributedObjectName() {
        return parameters.name;
    }
}
