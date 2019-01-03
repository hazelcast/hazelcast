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

package com.hazelcast.client.impl.protocol.task.transaction;

import com.hazelcast.client.impl.protocol.ClientMessage;
import com.hazelcast.client.impl.protocol.codec.XATransactionCommitCodec;
import com.hazelcast.client.impl.protocol.task.AbstractCallableMessageTask;
import com.hazelcast.client.impl.protocol.task.TransactionalMessageTask;
import com.hazelcast.instance.Node;
import com.hazelcast.nio.Connection;
import com.hazelcast.security.permission.TransactionPermission;
import com.hazelcast.transaction.TransactionContext;
import com.hazelcast.transaction.TransactionException;
import com.hazelcast.transaction.impl.Transaction;
import com.hazelcast.transaction.impl.xa.TransactionAccessor;
import com.hazelcast.transaction.impl.xa.XAService;

import java.security.Permission;

public class XATransactionCommitMessageTask
        extends AbstractCallableMessageTask<XATransactionCommitCodec.RequestParameters>
        implements TransactionalMessageTask {
    public XATransactionCommitMessageTask(ClientMessage clientMessage, Node node, Connection connection) {
        super(clientMessage, node, connection);
    }

    @Override
    protected XATransactionCommitCodec.RequestParameters decodeClientMessage(ClientMessage clientMessage) {
        return XATransactionCommitCodec.decodeRequest(clientMessage);
    }

    @Override
    protected ClientMessage encodeResponse(Object response) {
        return XATransactionCommitCodec.encodeResponse();
    }

    @Override
    protected Object call() throws Exception {
        String transactionId = parameters.transactionId;
        TransactionContext transactionContext = endpoint.getTransactionContext(transactionId);
        if (transactionContext == null) {
            throw new TransactionException("No transaction context with given transactionId: " + transactionId);
        }
        Transaction transaction = TransactionAccessor.getTransaction(transactionContext);
        if (parameters.onePhase) {
            transaction.prepare();
        }
        transaction.commit();
        endpoint.removeTransactionContext(transactionId);
        return null;
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
