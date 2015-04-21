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

package com.hazelcast.client.impl.protocol.task.transactionalset;

import com.hazelcast.client.impl.protocol.ClientMessage;
import com.hazelcast.client.impl.protocol.parameters.BooleanResultParameters;
import com.hazelcast.client.impl.protocol.parameters.TransactionalSetRemoveParameters;
import com.hazelcast.client.impl.protocol.task.AbstractTransactionalMessageTask;
import com.hazelcast.collection.impl.set.SetService;
import com.hazelcast.core.TransactionalSet;
import com.hazelcast.instance.Node;
import com.hazelcast.nio.Connection;
import com.hazelcast.security.permission.ActionConstants;
import com.hazelcast.security.permission.SetPermission;
import com.hazelcast.transaction.TransactionContext;

import java.security.Permission;

public class TransactionalSetRemoveMessageTask
        extends AbstractTransactionalMessageTask<TransactionalSetRemoveParameters> {

    public TransactionalSetRemoveMessageTask(ClientMessage clientMessage, Node node, Connection connection) {
        super(clientMessage, node, connection);
    }

    @Override
    protected ClientMessage innerCall() throws Exception {
        final TransactionContext context = endpoint.getTransactionContext(parameters.txnId);
        TransactionalSet<Object> set = context.getSet(parameters.name);
        boolean success = set.remove(parameters.item);
        return BooleanResultParameters.encode(success);
    }

    @Override
    protected long getClientThreadId() {
        return parameters.threadId;
    }

    @Override
    protected TransactionalSetRemoveParameters decodeClientMessage(ClientMessage clientMessage) {
        return TransactionalSetRemoveParameters.decode(clientMessage);
    }

    @Override
    public String getServiceName() {
        return SetService.SERVICE_NAME;
    }

    @Override
    public Permission getRequiredPermission() {
        return new SetPermission(parameters.name, ActionConstants.ACTION_REMOVE);
    }

    @Override
    public String getDistributedObjectName() {
        return parameters.name;
    }

    @Override
    public String getMethodName() {
        return "remove";
    }

    @Override
    public Object[] getParameters() {
        return new Object[]{parameters.item};
    }
}
