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

package com.hazelcast.raft.service.semaphore.client;

import com.hazelcast.client.impl.protocol.ClientMessage;
import com.hazelcast.client.impl.protocol.task.AbstractMessageTask;
import com.hazelcast.core.ExecutionCallback;
import com.hazelcast.instance.Node;
import com.hazelcast.nio.Bits;
import com.hazelcast.nio.Connection;
import com.hazelcast.raft.RaftGroupId;
import com.hazelcast.raft.impl.RaftGroupIdImpl;
import com.hazelcast.raft.impl.service.RaftInvocationManager;
import com.hazelcast.raft.impl.service.RaftService;
import com.hazelcast.raft.service.semaphore.RaftSemaphoreService;

import java.security.Permission;

/**
 * Base class for client message tasks of Raft-based semaphore
 */
public abstract class AbstractSemaphoreMessageTask extends AbstractMessageTask implements ExecutionCallback {

    protected RaftGroupId groupId;
    protected String name;
    protected long sessionId;

    AbstractSemaphoreMessageTask(ClientMessage clientMessage, Node node, Connection connection) {
        super(clientMessage, node, connection);
    }

    @Override
    protected Object decodeClientMessage(ClientMessage clientMessage) {
        groupId = RaftGroupIdImpl.readFrom(clientMessage);
        name = clientMessage.getStringUtf8();
        sessionId = clientMessage.getLong();
        return null;
    }

    RaftInvocationManager getRaftInvocationManager() {
        RaftService raftService = nodeEngine.getService(RaftService.SERVICE_NAME);
        return raftService.getInvocationManager();
    }

    @Override
    protected ClientMessage encodeResponse(Object response) {
        if (response instanceof Integer) {
            return encodeIntResponse((Integer) response);
        }
        if (response instanceof Boolean) {
            return encodeBooleanResponse((Boolean) response);
        }
        throw new IllegalArgumentException("Unknown response: " + response);
    }

    private ClientMessage encodeIntResponse(int response) {
        int dataSize = ClientMessage.HEADER_SIZE + Bits.INT_SIZE_IN_BYTES;
        ClientMessage clientMessage = ClientMessage.createForEncode(dataSize);
        clientMessage.set(response);
        clientMessage.updateFrameLength();
        return clientMessage;
    }

    private ClientMessage encodeBooleanResponse(boolean response) {
        int dataSize = ClientMessage.HEADER_SIZE + Bits.BOOLEAN_SIZE_IN_BYTES;
        ClientMessage clientMessage = ClientMessage.createForEncode(dataSize);
        clientMessage.set(response);
        clientMessage.updateFrameLength();
        return clientMessage;
    }

    @Override
    public void onResponse(Object response) {
        sendResponse(response);
    }

    @Override
    public void onFailure(Throwable t) {
        handleProcessingFailure(t);
    }

    @Override
    public String getServiceName() {
        return RaftSemaphoreService.SERVICE_NAME;
    }

    @Override
    public String getDistributedObjectName() {
        return name;
    }

    @Override
    public Permission getRequiredPermission() {
        return null;
    }

    @Override
    public String getMethodName() {
        return null;
    }

    @Override
    public Object[] getParameters() {
        return new Object[0];
    }
}
