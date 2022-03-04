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

import com.hazelcast.client.impl.protocol.ClientMessage;
import com.hazelcast.client.impl.protocol.codec.XATransactionCreateCodec;
import com.hazelcast.client.impl.protocol.task.AbstractCallableMessageTask;
import com.hazelcast.instance.impl.Node;
import com.hazelcast.internal.nio.Connection;
import com.hazelcast.security.permission.TransactionPermission;
import com.hazelcast.transaction.TransactionContext;
import com.hazelcast.transaction.impl.xa.TransactionAccessor;
import com.hazelcast.transaction.impl.xa.XAService;

import java.security.Permission;
import java.util.UUID;

public class XATransactionCreateMessageTask
        extends AbstractCallableMessageTask<XATransactionCreateCodec.RequestParameters> {

    public XATransactionCreateMessageTask(ClientMessage clientMessage, Node node, Connection connection) {
        super(clientMessage, node, connection);
    }

    @Override
    protected Object call() throws Exception {
        XAService xaService = getService(getServiceName());
        UUID ownerUuid = endpoint.getUuid();
        TransactionContext context = xaService.newXATransactionContext(parameters.xid, ownerUuid, (int) parameters.timeout, true);
        TransactionAccessor.getTransaction(context).begin();
        endpoint.setTransactionContext(context);
        return context.getTxnId();
    }

    @Override
    protected XATransactionCreateCodec.RequestParameters decodeClientMessage(ClientMessage clientMessage) {
        return XATransactionCreateCodec.decodeRequest(clientMessage);
    }

    @Override
    protected ClientMessage encodeResponse(Object response) {
        return XATransactionCreateCodec.encodeResponse((UUID) response);
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
