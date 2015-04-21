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

package com.hazelcast.client.impl.protocol.task.transactionalqueue;

import com.hazelcast.client.impl.protocol.ClientMessage;
import com.hazelcast.client.impl.protocol.parameters.GenericResultParameters;
import com.hazelcast.client.impl.protocol.parameters.TransactionalQueuePeekParameters;
import com.hazelcast.client.impl.protocol.task.AbstractTransactionalMessageTask;
import com.hazelcast.collection.impl.queue.QueueService;
import com.hazelcast.core.TransactionalQueue;
import com.hazelcast.instance.Node;
import com.hazelcast.nio.Connection;
import com.hazelcast.security.permission.ActionConstants;
import com.hazelcast.security.permission.QueuePermission;
import com.hazelcast.transaction.TransactionContext;

import java.security.Permission;
import java.util.concurrent.TimeUnit;

public class TransactionalQueuePeekMessageTask
        extends AbstractTransactionalMessageTask<TransactionalQueuePeekParameters> {

    public TransactionalQueuePeekMessageTask(ClientMessage clientMessage, Node node, Connection connection) {
        super(clientMessage, node, connection);
    }

    @Override
    protected ClientMessage innerCall() throws Exception {
        final TransactionContext context = endpoint.getTransactionContext(parameters.txnId);
        final TransactionalQueue queue = context.getQueue(parameters.name);
        Object item = queue.peek(parameters.timeout, TimeUnit.MILLISECONDS);
        return GenericResultParameters.encode(serializationService.toData(item));
    }

    @Override
    protected long getClientThreadId() {
        return parameters.threadId;
    }

    @Override
    protected TransactionalQueuePeekParameters decodeClientMessage(ClientMessage clientMessage) {
        return TransactionalQueuePeekParameters.decode(clientMessage);
    }

    @Override
    public String getServiceName() {
        return QueueService.SERVICE_NAME;
    }

    @Override
    public Permission getRequiredPermission() {
        return new QueuePermission(parameters.name, ActionConstants.ACTION_READ);
    }

    @Override
    public String getDistributedObjectName() {
        return parameters.name;
    }

    @Override
    public String getMethodName() {
        return "peek";
    }

    @Override
    public Object[] getParameters() {
        return new Object[]{parameters.timeout, TimeUnit.MILLISECONDS};
    }
}
