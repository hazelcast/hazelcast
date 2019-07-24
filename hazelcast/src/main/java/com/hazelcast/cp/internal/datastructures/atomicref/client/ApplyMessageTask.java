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

package com.hazelcast.cp.internal.datastructures.atomicref.client;

import com.hazelcast.client.impl.protocol.ClientMessage;
import com.hazelcast.client.impl.protocol.codec.CPAtomicRefApplyCodec;
import com.hazelcast.client.impl.protocol.task.AbstractMessageTask;
import com.hazelcast.core.ExecutionCallback;
import com.hazelcast.core.ICompletableFuture;
import com.hazelcast.cp.CPGroupId;
import com.hazelcast.cp.internal.RaftInvocationManager;
import com.hazelcast.cp.internal.RaftOp;
import com.hazelcast.cp.internal.RaftService;
import com.hazelcast.cp.internal.datastructures.atomicref.RaftAtomicRefService;
import com.hazelcast.cp.internal.datastructures.atomicref.operation.ApplyOp;
import com.hazelcast.cp.internal.datastructures.atomicref.operation.ApplyOp.ReturnValueType;
import com.hazelcast.instance.impl.Node;
import com.hazelcast.nio.Connection;
import com.hazelcast.security.permission.ActionConstants;
import com.hazelcast.security.permission.AtomicReferencePermission;

import java.security.Permission;

import static com.hazelcast.cp.internal.raft.QueryPolicy.LINEARIZABLE;

/**
 * Client message task for {@link ApplyOp}
 */
public class ApplyMessageTask extends AbstractMessageTask<CPAtomicRefApplyCodec.RequestParameters>
        implements ExecutionCallback<Object> {

    public ApplyMessageTask(ClientMessage clientMessage, Node node, Connection connection) {
        super(clientMessage, node, connection);
    }

    @Override
    protected void processMessage() {
        ReturnValueType returnValueType = ReturnValueType.fromValue(parameters.returnValueType);
        RaftService service = nodeEngine.getService(RaftService.SERVICE_NAME);
        RaftInvocationManager invocationManager = service.getInvocationManager();
        CPGroupId groupId = parameters.groupId;
        RaftOp op = new ApplyOp(parameters.name, parameters.function, returnValueType, parameters.alter);
        ICompletableFuture<Object> future = parameters.alter
                ? invocationManager.invoke(groupId, op) : invocationManager.query(groupId, op, LINEARIZABLE);
        future.andThen(this);
    }

    @Override
    protected CPAtomicRefApplyCodec.RequestParameters decodeClientMessage(ClientMessage clientMessage) {
        return CPAtomicRefApplyCodec.decodeRequest(clientMessage);
    }

    @Override
    protected ClientMessage encodeResponse(Object response) {
        return CPAtomicRefApplyCodec.encodeResponse(serializationService.toData(response));
    }

    @Override
    public String getServiceName() {
        return RaftAtomicRefService.SERVICE_NAME;
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

    @Override
    public void onResponse(Object response) {
        sendResponse(response);
    }

    @Override
    public void onFailure(Throwable t) {
        handleProcessingFailure(t);
    }
}
