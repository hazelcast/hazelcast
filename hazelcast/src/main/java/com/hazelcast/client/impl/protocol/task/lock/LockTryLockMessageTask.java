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

package com.hazelcast.client.impl.protocol.task.lock;

import com.hazelcast.client.impl.protocol.ClientMessage;
import com.hazelcast.client.impl.protocol.parameters.BooleanResultParameters;
import com.hazelcast.client.impl.protocol.parameters.LockTryLockParameters;
import com.hazelcast.client.impl.protocol.task.AbstractPartitionMessageTask;
import com.hazelcast.concurrent.lock.InternalLockNamespace;
import com.hazelcast.concurrent.lock.LockService;
import com.hazelcast.concurrent.lock.operations.LockOperation;
import com.hazelcast.instance.Node;
import com.hazelcast.nio.Connection;
import com.hazelcast.nio.serialization.Data;
import com.hazelcast.security.permission.ActionConstants;
import com.hazelcast.security.permission.LockPermission;
import com.hazelcast.spi.Operation;

import java.security.Permission;
import java.util.concurrent.TimeUnit;

public class LockTryLockMessageTask extends AbstractPartitionMessageTask<LockTryLockParameters> {

    public LockTryLockMessageTask(ClientMessage clientMessage, Node node, Connection connection) {
        super(clientMessage, node, connection);
    }

    @Override
    protected Operation prepareOperation() {
        final Data key = serializationService.toData(parameters.name);
        return new LockOperation(new InternalLockNamespace(parameters.name)
                , key, parameters.threadId, -1, parameters.timeout);
    }

    @Override
    protected LockTryLockParameters decodeClientMessage(ClientMessage clientMessage) {
        return LockTryLockParameters.decode(clientMessage);
    }

    @Override
    protected ClientMessage encodeResponse(Object response) {
        return BooleanResultParameters.encode((Boolean) response);
    }

    @Override
    public String getServiceName() {
        return LockService.SERVICE_NAME;
    }

    @Override
    public Permission getRequiredPermission() {
        return new LockPermission(parameters.name, ActionConstants.ACTION_LOCK);
    }

    @Override
    public String getDistributedObjectName() {
        return parameters.name;
    }

    @Override
    public String getMethodName() {
        return "tryLock";
    }

    @Override
    public Object[] getParameters() {
        if (parameters.timeout == -1) {
            return null;
        }
        return new Object[]{parameters.timeout, TimeUnit.MILLISECONDS};
    }
}

