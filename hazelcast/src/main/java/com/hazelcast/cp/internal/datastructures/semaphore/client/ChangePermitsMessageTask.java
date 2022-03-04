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

package com.hazelcast.cp.internal.datastructures.semaphore.client;

import com.hazelcast.client.impl.protocol.ClientMessage;
import com.hazelcast.client.impl.protocol.codec.SemaphoreChangeCodec;
import com.hazelcast.cp.internal.RaftOp;
import com.hazelcast.cp.internal.client.AbstractCPMessageTask;
import com.hazelcast.cp.internal.datastructures.semaphore.SemaphoreService;
import com.hazelcast.cp.internal.datastructures.semaphore.operation.ChangePermitsOp;
import com.hazelcast.instance.impl.Node;
import com.hazelcast.internal.nio.Connection;
import com.hazelcast.security.permission.ActionConstants;
import com.hazelcast.security.permission.SemaphorePermission;

import java.security.Permission;

import static java.lang.Math.abs;

/**
 * Client message task for {@link ChangePermitsOp}
 */
public class ChangePermitsMessageTask extends AbstractCPMessageTask<SemaphoreChangeCodec.RequestParameters> {

    public ChangePermitsMessageTask(ClientMessage clientMessage, Node node, Connection connection) {
        super(clientMessage, node, connection);
    }

    @Override
    protected void processMessage() {
        RaftOp op = new ChangePermitsOp(parameters.name, parameters.sessionId, parameters.threadId, parameters.invocationUid,
                parameters.permits);
        invoke(parameters.groupId, op);
    }

    @Override
    protected SemaphoreChangeCodec.RequestParameters decodeClientMessage(ClientMessage clientMessage) {
        return SemaphoreChangeCodec.decodeRequest(clientMessage);
    }

    @Override
    protected ClientMessage encodeResponse(Object response) {
        return SemaphoreChangeCodec.encodeResponse((Boolean) response);
    }

    @Override
    public String getServiceName() {
        return SemaphoreService.SERVICE_NAME;
    }

    @Override
    public Permission getRequiredPermission() {
        return parameters.permits < 0 ? new SemaphorePermission(parameters.name, ActionConstants.ACTION_ACQUIRE)
                : new SemaphorePermission(parameters.name, ActionConstants.ACTION_RELEASE);
    }

    @Override
    public String getDistributedObjectName() {
        return parameters.name;
    }

    @Override
    public String getMethodName() {
        return parameters.permits > 0 ? "increasePermits" : "reducePermits";
    }

    @Override
    public Object[] getParameters() {
        return new Object[]{abs(parameters.permits)};
    }
}
