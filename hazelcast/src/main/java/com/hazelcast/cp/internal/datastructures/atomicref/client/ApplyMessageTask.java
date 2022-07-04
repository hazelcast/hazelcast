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

package com.hazelcast.cp.internal.datastructures.atomicref.client;

import com.hazelcast.client.impl.protocol.ClientMessage;
import com.hazelcast.client.impl.protocol.codec.AtomicRefApplyCodec;
import com.hazelcast.cp.CPGroupId;
import com.hazelcast.cp.internal.RaftOp;
import com.hazelcast.cp.internal.client.AbstractCPMessageTask;
import com.hazelcast.cp.internal.datastructures.atomicref.AtomicRefService;
import com.hazelcast.cp.internal.datastructures.atomicref.operation.ApplyOp;
import com.hazelcast.cp.internal.datastructures.atomicref.operation.ApplyOp.ReturnValueType;
import com.hazelcast.instance.impl.Node;
import com.hazelcast.internal.nio.Connection;
import com.hazelcast.security.permission.ActionConstants;
import com.hazelcast.security.permission.AtomicReferencePermission;

import java.security.Permission;

import static com.hazelcast.cp.internal.raft.QueryPolicy.LINEARIZABLE;

/**
 * Client message task for {@link ApplyOp}
 */
public class ApplyMessageTask extends AbstractCPMessageTask<AtomicRefApplyCodec.RequestParameters> {

    public ApplyMessageTask(ClientMessage clientMessage, Node node, Connection connection) {
        super(clientMessage, node, connection);
    }

    @Override
    protected void processMessage() {
        ReturnValueType returnValueType = ReturnValueType.fromValue(parameters.returnValueType);
        CPGroupId groupId = parameters.groupId;
        RaftOp op = new ApplyOp(parameters.name, parameters.function, returnValueType, parameters.alter);

        if (parameters.alter) {
            invoke(groupId, op);
        } else {
            query(groupId, op, LINEARIZABLE);
        }
    }

    @Override
    protected AtomicRefApplyCodec.RequestParameters decodeClientMessage(ClientMessage clientMessage) {
        return AtomicRefApplyCodec.decodeRequest(clientMessage);
    }

    @Override
    protected ClientMessage encodeResponse(Object response) {
        return AtomicRefApplyCodec.encodeResponse(serializationService.toData(response));
    }

    @Override
    public String getServiceName() {
        return AtomicRefService.SERVICE_NAME;
    }

    @Override
    public Permission getRequiredPermission() {
        return new AtomicReferencePermission(parameters.name, ActionConstants.ACTION_MODIFY);
    }

    @Override
    public String getDistributedObjectName() {
        return parameters.name;
    }

    @Override
    public String getMethodName() {
        if (parameters.alter) {
            if (parameters.returnValueType == ReturnValueType.RETURN_OLD_VALUE.value()) {
                return "getAndAlter";
            } else if (parameters.returnValueType == ReturnValueType.RETURN_NEW_VALUE.value()) {
                return "alterAndGet";
            }
            return "alter";
        }
        return "apply";
    }

    @Override
    public Object[] getParameters() {
        return new Object[]{parameters.function};
    }
}
