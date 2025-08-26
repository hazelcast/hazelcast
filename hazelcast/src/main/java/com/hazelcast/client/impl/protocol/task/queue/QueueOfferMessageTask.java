/*
 * Copyright (c) 2008-2025, Hazelcast, Inc. All Rights Reserved.
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
import com.hazelcast.client.impl.protocol.codec.QueueOfferCodec;
import com.hazelcast.collection.impl.queue.operations.OfferOperation;
import com.hazelcast.instance.impl.Node;
import com.hazelcast.internal.nio.Connection;
import com.hazelcast.security.SecurityInterceptorConstants;
import com.hazelcast.security.permission.ActionConstants;
import com.hazelcast.security.permission.QueuePermission;
import com.hazelcast.spi.impl.operationservice.Operation;

import java.security.Permission;
import java.util.concurrent.TimeUnit;

/**
 * Client Protocol Task for handling messages with type ID:
 * {@link com.hazelcast.client.impl.protocol.codec.QueueOfferCodec#REQUEST_MESSAGE_TYPE}
 */
public class QueueOfferMessageTask
        extends AbstractQueueMessageTask<QueueOfferCodec.RequestParameters> {

    public QueueOfferMessageTask(ClientMessage clientMessage, Node node, Connection connection) {
        super(clientMessage, node, connection);
    }

    @Override
    protected Operation prepareOperation() {
        return new OfferOperation(parameters.name, parameters.timeoutMillis, parameters.value);
    }

    @Override
    protected ClientMessage encodeResponse(Object response) {
        return QueueOfferCodec.encodeResponse((Boolean) response);
    }

    @Override
    protected QueueOfferCodec.RequestParameters decodeClientMessage(ClientMessage clientMessage) {
        return QueueOfferCodec.decodeRequest(clientMessage);
    }

    @Override
    public Object[] getParameters() {
        if (parameters.timeoutMillis > 0) {
            return new Object[]{parameters.value, parameters.timeoutMillis, TimeUnit.MILLISECONDS};
        }
        return new Object[]{parameters.value};
    }

    @Override
    public Permission getRequiredPermission() {
        return new QueuePermission(parameters.name, ActionConstants.ACTION_ADD);
    }

    @Override
    public String getMethodName() {
        return SecurityInterceptorConstants.OFFER;
    }

    @Override
    public String getDistributedObjectName() {
        return parameters.name;
    }
}
