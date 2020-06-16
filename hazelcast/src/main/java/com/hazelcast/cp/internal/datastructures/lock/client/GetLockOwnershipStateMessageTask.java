/*
 * Copyright (c) 2008-2019, Hazelcast, Inc. All Rights Reserved.
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
import com.hazelcast.client.impl.protocol.codec.CPFencedLockGetLockOwnershipCodec;
import com.hazelcast.client.impl.protocol.codec.CPFencedLockGetLockOwnershipCodec.RequestParameters;
import com.hazelcast.cp.internal.client.AbstractCPMessageTask;
import com.hazelcast.cp.internal.datastructures.lock.RaftLockOwnershipState;
import com.hazelcast.cp.internal.datastructures.lock.RaftLockService;
import com.hazelcast.cp.internal.datastructures.lock.operation.GetLockOwnershipStateOp;
import com.hazelcast.cp.internal.raft.QueryPolicy;
import com.hazelcast.instance.Node;
import com.hazelcast.nio.Connection;
import com.hazelcast.security.permission.ActionConstants;
import com.hazelcast.security.permission.LockPermission;

import java.security.Permission;

/**
 * Client message task for {@link GetLockOwnershipStateOp}
 */
public class GetLockOwnershipStateMessageTask extends AbstractCPMessageTask<RequestParameters> {

    public GetLockOwnershipStateMessageTask(ClientMessage clientMessage, Node node, Connection connection) {
        super(clientMessage, node, connection);
    }

    @Override
    protected void processMessage() {
        query(parameters.groupId, new GetLockOwnershipStateOp(parameters.name), QueryPolicy.LINEARIZABLE);
    }

    @Override
    protected CPFencedLockGetLockOwnershipCodec.RequestParameters decodeClientMessage(ClientMessage clientMessage) {
        return CPFencedLockGetLockOwnershipCodec.decodeRequest(clientMessage);
    }

    @Override
    protected ClientMessage encodeResponse(Object response) {
        RaftLockOwnershipState lockState = (RaftLockOwnershipState) response;
        return CPFencedLockGetLockOwnershipCodec.encodeResponse(lockState.getFence(), lockState.getLockCount(),
                lockState.getSessionId(), lockState.getThreadId());
    }

    @Override
    public String getServiceName() {
        return RaftLockService.SERVICE_NAME;
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
