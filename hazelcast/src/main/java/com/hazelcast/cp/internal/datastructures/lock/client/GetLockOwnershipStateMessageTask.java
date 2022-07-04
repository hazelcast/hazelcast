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

package com.hazelcast.cp.internal.datastructures.lock.client;

import com.hazelcast.client.impl.protocol.ClientMessage;
import com.hazelcast.client.impl.protocol.codec.FencedLockGetLockOwnershipCodec;
import com.hazelcast.cp.internal.client.AbstractCPMessageTask;
import com.hazelcast.cp.internal.datastructures.lock.LockOwnershipState;
import com.hazelcast.cp.internal.datastructures.lock.LockService;
import com.hazelcast.cp.internal.datastructures.lock.operation.GetLockOwnershipStateOp;
import com.hazelcast.instance.impl.Node;
import com.hazelcast.internal.nio.Connection;
import com.hazelcast.security.permission.ActionConstants;
import com.hazelcast.security.permission.LockPermission;

import java.security.Permission;

import static com.hazelcast.cp.internal.raft.QueryPolicy.LINEARIZABLE;

/**
 * Client message task for {@link GetLockOwnershipStateOp}
 */
public class GetLockOwnershipStateMessageTask extends AbstractCPMessageTask<FencedLockGetLockOwnershipCodec.RequestParameters> {

    public GetLockOwnershipStateMessageTask(ClientMessage clientMessage, Node node, Connection connection) {
        super(clientMessage, node, connection);
    }

    @Override
    protected void processMessage() {
        query(parameters.groupId, new GetLockOwnershipStateOp(parameters.name), LINEARIZABLE);
    }

    @Override
    protected FencedLockGetLockOwnershipCodec.RequestParameters decodeClientMessage(ClientMessage clientMessage) {
        return FencedLockGetLockOwnershipCodec.decodeRequest(clientMessage);
    }

    @Override
    protected ClientMessage encodeResponse(Object response) {
        LockOwnershipState lockState = (LockOwnershipState) response;
        return FencedLockGetLockOwnershipCodec.encodeResponse(lockState.getFence(), lockState.getLockCount(),
                lockState.getSessionId(), lockState.getThreadId());
    }

    @Override
    public String getServiceName() {
        return LockService.SERVICE_NAME;
    }

    @Override
    public Permission getRequiredPermission() {
        return new LockPermission(parameters.name, ActionConstants.ACTION_READ);
    }

    @Override
    public String getDistributedObjectName() {
        return parameters.name;
    }

    @Override
    public String getMethodName() {
        return "getLockOwnershipState";
    }

    @Override
    public Object[] getParameters() {
        return new Object[0];
    }
}
