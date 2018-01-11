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

package com.hazelcast.client.impl.protocol.task.transaction;

import com.hazelcast.client.ClientEndpoint;
import com.hazelcast.client.impl.protocol.ClientMessage;
import com.hazelcast.client.impl.protocol.codec.XATransactionClearRemoteCodec;
import com.hazelcast.client.impl.protocol.task.AbstractCallableMessageTask;
import com.hazelcast.instance.Node;
import com.hazelcast.internal.partition.InternalPartitionService;
import com.hazelcast.nio.Connection;
import com.hazelcast.nio.serialization.Data;
import com.hazelcast.security.permission.TransactionPermission;
import com.hazelcast.spi.InvocationBuilder;
import com.hazelcast.spi.Operation;
import com.hazelcast.spi.impl.operationservice.InternalOperationService;
import com.hazelcast.transaction.impl.xa.XAService;
import com.hazelcast.transaction.impl.xa.operations.ClearRemoteTransactionOperation;

import java.security.Permission;

public class XAClearRemoteTransactionMessageTask
        extends AbstractCallableMessageTask<XATransactionClearRemoteCodec.RequestParameters> {

    private static final int TRY_COUNT = 100;

    public XAClearRemoteTransactionMessageTask(ClientMessage clientMessage, Node node, Connection connection) {
        super(clientMessage, node, connection);
    }

    @Override
    protected XATransactionClearRemoteCodec.RequestParameters decodeClientMessage(ClientMessage clientMessage) {
        return XATransactionClearRemoteCodec.decodeRequest(clientMessage);
    }

    @Override
    protected ClientMessage encodeResponse(Object response) {
        return XATransactionClearRemoteCodec.encodeResponse();
    }

    @Override
    protected Object call() throws Exception {
        InternalOperationService operationService = nodeEngine.getOperationService();
        InternalPartitionService partitionService = nodeEngine.getPartitionService();
        ClientEndpoint endpoint = getEndpoint();

        Data xidData = serializationService.toData(parameters.xid);
        Operation op = new ClearRemoteTransactionOperation(xidData);
        op.setCallerUuid(endpoint.getUuid());
        int partitionId = partitionService.getPartitionId(xidData);

        InvocationBuilder builder = operationService.createInvocationBuilder(getServiceName(), op, partitionId);
        builder.setTryCount(TRY_COUNT).setResultDeserialized(false);
        builder.invoke();
        return XATransactionClearRemoteCodec.encodeResponse();
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
