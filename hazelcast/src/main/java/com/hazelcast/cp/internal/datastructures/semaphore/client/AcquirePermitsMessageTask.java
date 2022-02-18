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
import com.hazelcast.client.impl.protocol.codec.SemaphoreAcquireCodec;
import com.hazelcast.cp.internal.RaftOp;
import com.hazelcast.cp.internal.client.AbstractCPMessageTask;
import com.hazelcast.cp.internal.datastructures.semaphore.SemaphoreService;
import com.hazelcast.cp.internal.datastructures.semaphore.operation.AcquirePermitsOp;
import com.hazelcast.instance.impl.Node;
import com.hazelcast.internal.nio.Connection;
import com.hazelcast.security.permission.ActionConstants;
import com.hazelcast.security.permission.SemaphorePermission;

import java.security.Permission;
import java.util.concurrent.TimeUnit;

/**
 * Client message task for {@link AcquirePermitsOp}
 */
public class AcquirePermitsMessageTask extends AbstractCPMessageTask<SemaphoreAcquireCodec.RequestParameters> {

    public AcquirePermitsMessageTask(ClientMessage clientMessage, Node node, Connection connection) {
        super(clientMessage, node, connection);
    }

    @Override
    protected void processMessage() {
        RaftOp op = new AcquirePermitsOp(parameters.name, parameters.sessionId, parameters.threadId, parameters.invocationUid,
                parameters.permits, parameters.timeoutMs);
        invoke(parameters.groupId, op);
    }

    @Override
    protected SemaphoreAcquireCodec.RequestParameters decodeClientMessage(ClientMessage clientMessage) {
        return SemaphoreAcquireCodec.decodeRequest(clientMessage);
    }

    @Override
    protected ClientMessage encodeResponse(Object response) {
        return SemaphoreAcquireCodec.encodeResponse((Boolean) response);
    }

    @Override
    public String getServiceName() {
        return SemaphoreService.SERVICE_NAME;
    }

    @Override
    public Permission getRequiredPermission() {
        return new SemaphorePermission(parameters.name, ActionConstants.ACTION_ACQUIRE);
    }

    @Override
    public String getDistributedObjectName() {
        return parameters.name;
    }

    @Override
    public String getMethodName() {
        return parameters.timeoutMs >= 0 ? "tryAcquire" : "acquire";
    }

    @Override
    public Object[] getParameters() {
        if (parameters.timeoutMs > 0) {
            return new Object[]{parameters.permits, parameters.timeoutMs, TimeUnit.MILLISECONDS};
        }
        return new Object[]{parameters.permits};
    }
}
