/*
 * Copyright (c) 2008-2018, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.client.impl.protocol.task.transactionalmap;

import com.hazelcast.client.impl.protocol.ClientMessage;
import com.hazelcast.client.impl.protocol.codec.TransactionalMapReplaceCodec;
import com.hazelcast.client.impl.protocol.task.AbstractTransactionalMessageTask;
import com.hazelcast.core.TransactionalMap;
import com.hazelcast.instance.Node;
import com.hazelcast.map.impl.MapService;
import com.hazelcast.nio.Connection;
import com.hazelcast.security.permission.ActionConstants;
import com.hazelcast.security.permission.MapPermission;
import com.hazelcast.transaction.TransactionContext;

import java.security.Permission;

public class TransactionalMapReplaceMessageTask
        extends AbstractTransactionalMessageTask<TransactionalMapReplaceCodec.RequestParameters> {

    public TransactionalMapReplaceMessageTask(ClientMessage clientMessage, Node node, Connection connection) {
        super(clientMessage, node, connection);
    }

    @Override
    protected Object innerCall() throws Exception {
        final TransactionContext context = getEndpoint().getTransactionContext(parameters.txnId);
        final TransactionalMap map = context.getMap(parameters.name);
        Object oldValue = map.replace(parameters.key, parameters.value);
        return serializationService.toData(oldValue);
    }

    @Override
    protected long getClientThreadId() {
        return parameters.threadId;
    }

    @Override
    protected TransactionalMapReplaceCodec.RequestParameters decodeClientMessage(ClientMessage clientMessage) {
        return TransactionalMapReplaceCodec.decodeRequest(clientMessage);
    }

    @Override
    protected ClientMessage encodeResponse(Object response) {
        return TransactionalMapReplaceCodec.encodeResponse(serializationService.toData(response));
    }

    @Override
    public String getServiceName() {
        return MapService.SERVICE_NAME;
    }

    @Override
    public Permission getRequiredPermission() {
        return new MapPermission(parameters.name, ActionConstants.ACTION_PUT, ActionConstants.ACTION_LOCK);
    }

    @Override
    public String getDistributedObjectName() {
        return parameters.name;
    }

    @Override
    public String getMethodName() {
        return "replace";
    }

    @Override
    public Object[] getParameters() {
        return new Object[]{parameters.key, parameters.value};
    }
}
