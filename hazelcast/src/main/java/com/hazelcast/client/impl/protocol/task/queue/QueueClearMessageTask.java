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

package com.hazelcast.client.impl.protocol.task.queue;

import com.hazelcast.client.impl.protocol.ClientMessage;
import com.hazelcast.client.impl.protocol.parameters.QueueClearParameters;
import com.hazelcast.client.impl.protocol.parameters.VoidResultParameters;
import com.hazelcast.client.impl.protocol.task.AbstractPartitionMessageTask;
import com.hazelcast.collection.impl.queue.QueueService;
import com.hazelcast.collection.impl.queue.operations.ClearOperation;
import com.hazelcast.instance.Node;
import com.hazelcast.nio.Connection;
import com.hazelcast.security.permission.ActionConstants;
import com.hazelcast.security.permission.QueuePermission;
import com.hazelcast.spi.Operation;

import java.security.Permission;

/**
 * Client Protocol Task for handling messages with type id:
 * {@link com.hazelcast.client.impl.protocol.parameters.QueueMessageType#QUEUE_CLEAR}
 *
 */
public class QueueClearMessageTask
        extends AbstractPartitionMessageTask<QueueClearParameters> {

    public QueueClearMessageTask(ClientMessage clientMessage, Node node, Connection connection) {
        super(clientMessage, node, connection);
    }

    @Override
    protected Operation prepareOperation() {
        return new ClearOperation(parameters.name);
    }

    @Override
    protected QueueClearParameters decodeClientMessage(ClientMessage clientMessage) {
        return QueueClearParameters.decode(clientMessage);
    }

    @Override
    protected ClientMessage encodeResponse(Object response) {
        return VoidResultParameters.encode();
    }

    @Override
    public Permission getRequiredPermission() {
        return new QueuePermission(parameters.name, ActionConstants.ACTION_REMOVE);
    }

    @Override
    public String getMethodName() {
        return "clear";
    }

    @Override
    public String getServiceName() {
        return QueueService.SERVICE_NAME;
    }

    @Override
    public Object[] getParameters() {
        return null;
    }

    @Override
    public String getDistributedObjectName() {
        return parameters.name;
    }
}
