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

package com.hazelcast.cp.internal.datastructures.atomiclong.client;

import com.hazelcast.client.impl.protocol.ClientMessage;
import com.hazelcast.client.impl.protocol.codec.CPAtomicLongAlterCodec;
import com.hazelcast.client.impl.protocol.task.AbstractMessageTask;
import com.hazelcast.core.ExecutionCallback;
import com.hazelcast.core.IFunction;
import com.hazelcast.cp.internal.RaftService;
import com.hazelcast.cp.internal.datastructures.atomiclong.RaftAtomicLongService;
import com.hazelcast.cp.internal.datastructures.atomiclong.operation.AlterOp;
import com.hazelcast.cp.internal.datastructures.atomiclong.operation.AlterOp.AlterResultType;
import com.hazelcast.instance.impl.Node;
import com.hazelcast.nio.Connection;
import com.hazelcast.security.permission.ActionConstants;
import com.hazelcast.security.permission.AtomicLongPermission;

import java.security.Permission;

/**
 * Client message task for {@link AlterOp}
 */
public class AlterMessageTask extends AbstractMessageTask<CPAtomicLongAlterCodec.RequestParameters>
        implements ExecutionCallback<Long> {

    public AlterMessageTask(ClientMessage clientMessage, Node node, Connection connection) {
        super(clientMessage, node, connection);
    }

    @Override
    protected void processMessage() {
        IFunction<Long, Long> function = serializationService.toObject(parameters.function);
        AlterResultType resultType = AlterResultType.fromValue(parameters.returnValueType);
        RaftService service = nodeEngine.getService(RaftService.SERVICE_NAME);
        service.getInvocationManager()
               .<Long>invoke(parameters.groupId, new AlterOp(parameters.name, function, resultType))
               .andThen(this);
    }

    @Override
    protected CPAtomicLongAlterCodec.RequestParameters decodeClientMessage(ClientMessage clientMessage) {
        return CPAtomicLongAlterCodec.decodeRequest(clientMessage);
    }

    @Override
    protected ClientMessage encodeResponse(Object response) {
        return CPAtomicLongAlterCodec.encodeResponse((Long) response);
    }

    @Override
    public String getServiceName() {
        return RaftAtomicLongService.SERVICE_NAME;
    }

    @Override
    public Permission getRequiredPermission() {
        return new AtomicLongPermission(parameters.name, ActionConstants.ACTION_MODIFY);
    }

    @Override
    public String getDistributedObjectName() {
        return parameters.name;
    }

    @Override
    public String getMethodName() {
        if (parameters.returnValueType == AlterResultType.OLD_VALUE.value()) {
            return "getAndAlter";
        }
        if (parameters.returnValueType == AlterResultType.NEW_VALUE.value()) {
            return "alterAndGet";
        }
        return "alter";
    }

    @Override
    public Object[] getParameters() {
        return new Object[]{parameters.function};
    }

    @Override
    public void onResponse(Long response) {
        sendResponse(response);
    }

    @Override
    public void onFailure(Throwable t) {
        handleProcessingFailure(t);
    }
}
