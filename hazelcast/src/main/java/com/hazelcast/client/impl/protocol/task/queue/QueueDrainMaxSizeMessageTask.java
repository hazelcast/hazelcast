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
import com.hazelcast.client.impl.protocol.codec.QueueDrainToMaxSizeCodec;
import com.hazelcast.collection.impl.queue.operations.DrainOperation;
import com.hazelcast.instance.impl.Node;
import com.hazelcast.internal.nio.Connection;
import com.hazelcast.internal.serialization.Data;
import com.hazelcast.security.SecurityInterceptorConstants;
import com.hazelcast.security.permission.ActionConstants;
import com.hazelcast.security.permission.QueuePermission;
import com.hazelcast.spi.impl.operationservice.Operation;
import com.hazelcast.spi.impl.SerializableList;

import java.security.Permission;
import java.util.List;

/**
 * Client Protocol Task for handling messages with type ID:
 * {@link com.hazelcast.client.impl.protocol.codec.QueueDrainToMaxSizeCodec#REQUEST_MESSAGE_TYPE}
 */
public class QueueDrainMaxSizeMessageTask
        extends AbstractQueueMessageTask<QueueDrainToMaxSizeCodec.RequestParameters> {

    public QueueDrainMaxSizeMessageTask(ClientMessage clientMessage, Node node, Connection connection) {
        super(clientMessage, node, connection);
    }

    @Override
    protected Operation prepareOperation() {
        return new DrainOperation(parameters.name, parameters.maxSize);
    }

    @Override
    protected QueueDrainToMaxSizeCodec.RequestParameters decodeClientMessage(ClientMessage clientMessage) {
        return QueueDrainToMaxSizeCodec.decodeRequest(clientMessage);
    }

    @Override
    public Permission getRequiredPermission() {
        return new QueuePermission(parameters.name, ActionConstants.ACTION_REMOVE);
    }

    @Override
    public String getMethodName() {
        return SecurityInterceptorConstants.DRAIN_TO;
    }

    @Override
    public Object[] getParameters() {
        return new Object[]{null, parameters.maxSize};
    }

    @Override
    protected ClientMessage encodeResponse(Object response) {
        SerializableList serializableList = (SerializableList) response;
        List<Data> coll = serializableList.getCollection();
        return QueueDrainToMaxSizeCodec.encodeResponse(coll);
    }

    @Override
    public String getDistributedObjectName() {
        return parameters.name;
    }
}
