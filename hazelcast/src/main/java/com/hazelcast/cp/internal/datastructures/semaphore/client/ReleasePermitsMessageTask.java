/*
 * Copyright (c) 2008-2020, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.cp.internal.datastructures.semaphore.client;

import com.hazelcast.client.impl.protocol.ClientMessage;
import com.hazelcast.client.impl.protocol.codec.CPSemaphoreReleaseCodec;
import com.hazelcast.client.impl.protocol.task.AbstractMessageTask;
import com.hazelcast.core.ExecutionCallback;
import com.hazelcast.cp.internal.RaftOp;
import com.hazelcast.cp.internal.RaftService;
import com.hazelcast.cp.internal.datastructures.semaphore.RaftSemaphoreService;
import com.hazelcast.cp.internal.datastructures.semaphore.operation.ReleasePermitsOp;
import com.hazelcast.instance.Node;
import com.hazelcast.nio.Connection;
import com.hazelcast.security.permission.ActionConstants;
import com.hazelcast.security.permission.SemaphorePermission;

import java.security.Permission;

/**
 * Client message task for {@link ReleasePermitsOp}
 */
public class ReleasePermitsMessageTask extends AbstractMessageTask<CPSemaphoreReleaseCodec.RequestParameters>
        implements ExecutionCallback<Boolean> {

    public ReleasePermitsMessageTask(ClientMessage clientMessage, Node node, Connection connection) {
        super(clientMessage, node, connection);
    }

    @Override
    protected void processMessage() {
        RaftService service = nodeEngine.getService(RaftService.SERVICE_NAME);
        RaftOp op = new ReleasePermitsOp(parameters.name, parameters.sessionId, parameters.threadId, parameters.invocationUid,
                parameters.permits);
        service.getInvocationManager()
               .<Boolean>invoke(parameters.groupId, op)
               .andThen(this);
    }

    @Override
    protected CPSemaphoreReleaseCodec.RequestParameters decodeClientMessage(ClientMessage clientMessage) {
        return CPSemaphoreReleaseCodec.decodeRequest(clientMessage);
    }

    @Override
    protected ClientMessage encodeResponse(Object response) {
        return CPSemaphoreReleaseCodec.encodeResponse((Boolean) response);
    }

    @Override
    public String getServiceName() {
        return RaftSemaphoreService.SERVICE_NAME;
    }

    @Override
    public Permission getRequiredPermission() {
        return new SemaphorePermission(parameters.name, ActionConstants.ACTION_RELEASE);
    }

    @Override
    public String getDistributedObjectName() {
        return parameters.name;
    }

    @Override
    public String getMethodName() {
        return "release";
    }

    @Override
    public Object[] getParameters() {
        return new Object[]{parameters.permits};
    }

    @Override
    public void onResponse(Boolean response) {
        sendResponse(response);
    }

    @Override
    public void onFailure(Throwable t) {
        handleProcessingFailure(t);
    }
}
