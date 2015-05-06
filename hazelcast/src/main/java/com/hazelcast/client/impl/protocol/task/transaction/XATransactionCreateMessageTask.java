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

package com.hazelcast.client.impl.protocol.task.transaction;

import com.hazelcast.client.ClientEndpoint;
import com.hazelcast.client.impl.protocol.ClientMessage;
import com.hazelcast.client.impl.protocol.parameters.TransactionCreateResultParameters;
import com.hazelcast.client.impl.protocol.parameters.XATransactionCreateParameters;
import com.hazelcast.client.impl.protocol.task.AbstractCallableMessageTask;
import com.hazelcast.instance.Node;
import com.hazelcast.nio.Connection;
import com.hazelcast.security.permission.TransactionPermission;
import com.hazelcast.transaction.TransactionContext;
import com.hazelcast.transaction.impl.xa.TransactionAccessor;
import com.hazelcast.transaction.impl.xa.XAService;

import java.security.Permission;

public class XATransactionCreateMessageTask
        extends AbstractCallableMessageTask<XATransactionCreateParameters> {

    public XATransactionCreateMessageTask(ClientMessage clientMessage, Node node, Connection connection) {
        super(clientMessage, node, connection);
    }

    @Override
    protected ClientMessage call() throws Exception {
        ClientEndpoint endpoint = getEndpoint();
        XAService xaService = getService(getServiceName());
        TransactionContext context = xaService.newXATransactionContext(parameters.xid, (int) parameters.timeout);
        TransactionAccessor.getTransaction(context).begin();
        endpoint.setTransactionContext(context);
        return TransactionCreateResultParameters.encode(context.getTxnId());
    }

    @Override
    protected XATransactionCreateParameters decodeClientMessage(ClientMessage clientMessage) {
        return XATransactionCreateParameters.decode(clientMessage);
    }

    @Override
    public String getServiceName() {
        return XAService.SERVICE_NAME;
    }

    @Override
    public String getDistributedObjectName() {
        return null;
    }

    @Override
    public String getMethodName() {
        return null;
    }

    @Override
    public Object[] getParameters() {
        return null;
    }

    @Override
    public Permission getRequiredPermission() {
        return new TransactionPermission();
    }
}
