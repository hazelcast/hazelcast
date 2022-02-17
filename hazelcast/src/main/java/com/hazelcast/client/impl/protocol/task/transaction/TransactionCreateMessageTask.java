/*
 * Copyright (c) 2008-2022, Hazelcast, Inc. All Rights Reserved.
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

import com.hazelcast.client.impl.ClientEngineImpl;
import com.hazelcast.client.impl.protocol.ClientMessage;
import com.hazelcast.client.impl.protocol.codec.TransactionCreateCodec;
import com.hazelcast.client.impl.protocol.task.AbstractTransactionalMessageTask;
import com.hazelcast.instance.impl.Node;
import com.hazelcast.internal.nio.Connection;
import com.hazelcast.security.permission.TransactionPermission;
import com.hazelcast.transaction.TransactionContext;
import com.hazelcast.transaction.TransactionOptions;
import com.hazelcast.transaction.impl.TransactionManagerServiceImpl;

import java.security.Permission;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

public class TransactionCreateMessageTask
        extends AbstractTransactionalMessageTask<TransactionCreateCodec.RequestParameters> {


    public TransactionCreateMessageTask(ClientMessage clientMessage, Node node, Connection connection) {
        super(clientMessage, node, connection);
    }

    @Override
    protected Object innerCall() throws Exception {
        TransactionOptions options = new TransactionOptions();
        options.setDurability(parameters.durability);
        options.setTimeout(parameters.timeout, TimeUnit.MILLISECONDS);
        options.setTransactionType(TransactionOptions.TransactionType.getById(parameters.transactionType));

        TransactionManagerServiceImpl transactionManager =
                (TransactionManagerServiceImpl) clientEngine.getTransactionManagerService();
        TransactionContext context = transactionManager.newClientTransactionContext(options, endpoint.getUuid());
        context.beginTransaction();
        endpoint.setTransactionContext(context);
        return context.getTxnId();
    }

    @Override
    protected long getClientThreadId() {
        return parameters.threadId;
    }

    @Override
    protected TransactionCreateCodec.RequestParameters decodeClientMessage(ClientMessage clientMessage) {
        return TransactionCreateCodec.decodeRequest(clientMessage);
    }

    @Override
    protected ClientMessage encodeResponse(Object response) {
        return TransactionCreateCodec.encodeResponse((UUID) response);
    }

    @Override
    public String getServiceName() {
        return ClientEngineImpl.SERVICE_NAME;
    }

    @Override
    public Permission getRequiredPermission() {
        return new TransactionPermission();
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
}
