/*
 * Copyright (c) 2008-2025, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.client.impl.protocol.task.multimap;

import com.hazelcast.client.impl.protocol.ClientMessage;
import com.hazelcast.client.impl.protocol.codec.MultiMapUnlockCodec;
import com.hazelcast.client.impl.protocol.task.AbstractPartitionMessageTask;
import com.hazelcast.internal.locksupport.LockSupportService;
import com.hazelcast.internal.locksupport.operations.UnlockOperation;
import com.hazelcast.instance.impl.Node;
import com.hazelcast.multimap.impl.MultiMapService;
import com.hazelcast.internal.nio.Connection;
import com.hazelcast.security.SecurityInterceptorConstants;
import com.hazelcast.security.permission.ActionConstants;
import com.hazelcast.security.permission.MultiMapPermission;
import com.hazelcast.internal.services.DistributedObjectNamespace;
import com.hazelcast.spi.impl.operationservice.Operation;

import java.security.Permission;

/**
 * Client Protocol Task for handling messages with type ID:
 * {@link com.hazelcast.client.impl.protocol.codec.MultiMapUnlockCodec#REQUEST_MESSAGE_TYPE}
 */
public class MultiMapUnlockMessageTask
        extends AbstractPartitionMessageTask<MultiMapUnlockCodec.RequestParameters> {

    public MultiMapUnlockMessageTask(ClientMessage clientMessage, Node node, Connection connection) {
        super(clientMessage, node, connection);
    }

    @Override
    protected Operation prepareOperation() {
        DistributedObjectNamespace namespace = new DistributedObjectNamespace(MultiMapService.SERVICE_NAME, parameters.name);
        return new UnlockOperation(namespace, parameters.key, parameters.threadId, false, parameters.referenceId);
    }

    @Override
    protected ClientMessage encodeResponse(Object response) {
        return MultiMapUnlockCodec.encodeResponse();
    }

    @Override
    protected MultiMapUnlockCodec.RequestParameters decodeClientMessage(ClientMessage clientMessage) {
        return MultiMapUnlockCodec.decodeRequest(clientMessage);
    }

    @Override
    public String getServiceName() {
        return LockSupportService.SERVICE_NAME;
    }

    @Override
    public String getDistributedObjectType() {
        return MultiMapService.SERVICE_NAME;
    }

    @Override
    public String getMethodName() {
        return SecurityInterceptorConstants.UNLOCK;
    }

    @Override
    public Object[] getParameters() {
        return new Object[]{parameters.key};
    }

    @Override
    public Permission getRequiredPermission() {
        return new MultiMapPermission(parameters.name, ActionConstants.ACTION_LOCK);
    }

    @Override
    public String getDistributedObjectName() {
        return parameters.name;
    }

    @Override
    protected String getUserCodeNamespace() {
        // This task is not Namespace-aware so it doesn't matter
        return null;
    }
}
