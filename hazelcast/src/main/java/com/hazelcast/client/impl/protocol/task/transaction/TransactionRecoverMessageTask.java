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

import com.hazelcast.client.impl.protocol.ClientMessage;
import com.hazelcast.client.impl.protocol.parameters.TransactionRecoverParameters;
import com.hazelcast.client.impl.protocol.parameters.VoidResultParameters;
import com.hazelcast.client.impl.protocol.task.AbstractCallableMessageTask;
import com.hazelcast.instance.Node;
import com.hazelcast.nio.Connection;
import com.hazelcast.security.permission.TransactionPermission;
import com.hazelcast.transaction.impl.TransactionManagerServiceImpl;

import java.security.Permission;

public class TransactionRecoverMessageTask extends AbstractCallableMessageTask<TransactionRecoverParameters> {

    public TransactionRecoverMessageTask(ClientMessage clientMessage, Node node, Connection connection) {
        super(clientMessage, node, connection);
    }

    @Override
    protected ClientMessage call() throws Exception {
        TransactionManagerServiceImpl service = getService(getServiceName());
        service.recoverClientTransaction(parameters.xid, parameters.commit);
        return VoidResultParameters.encode();
    }

    @Override
    protected TransactionRecoverParameters decodeClientMessage(ClientMessage clientMessage) {
        return TransactionRecoverParameters.decode(clientMessage);
    }

    @Override
    public String getServiceName() {
        return TransactionManagerServiceImpl.SERVICE_NAME;
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
