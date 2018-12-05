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
import com.hazelcast.client.impl.protocol.codec.CPAtomicLongCompareAndSetCodec;
import com.hazelcast.client.impl.protocol.task.AbstractMessageTask;
import com.hazelcast.core.ExecutionCallback;
import com.hazelcast.cp.internal.RaftService;
import com.hazelcast.cp.internal.datastructures.atomiclong.RaftAtomicLongService;
import com.hazelcast.cp.internal.datastructures.atomiclong.operation.CompareAndSetOp;
import com.hazelcast.instance.Node;
import com.hazelcast.nio.Connection;

import java.security.Permission;

/**
 * Client message task for {@link CompareAndSetOp}
 */
public class CompareAndSetMessageTask extends AbstractMessageTask<CPAtomicLongCompareAndSetCodec.RequestParameters>
        implements ExecutionCallback<Boolean> {

    public CompareAndSetMessageTask(ClientMessage clientMessage, Node node, Connection connection) {
        super(clientMessage, node, connection);
    }

    @Override
    protected void processMessage() {
        RaftService service = nodeEngine.getService(RaftService.SERVICE_NAME);
        service.getInvocationManager()
               .<Boolean>invoke(parameters.groupId, new CompareAndSetOp(parameters.name, parameters.expected, parameters.updated))
               .andThen(this);
    }

    @Override
    public String getServiceName() {
        return RaftAtomicLongService.SERVICE_NAME;
    }

    @Override
    public Permission getRequiredPermission() {
        return null;
    }

    @Override
    public String getDistributedObjectName() {
        return parameters.name;
    }

    @Override
    public String getMethodName() {
        return "compareAndSet";
    }

    @Override
    public Object[] getParameters() {
        return new Object[0];
    }

    @Override
    protected CPAtomicLongCompareAndSetCodec.RequestParameters decodeClientMessage(ClientMessage clientMessage) {
        return CPAtomicLongCompareAndSetCodec.decodeRequest(clientMessage);
    }

    @Override
    protected ClientMessage encodeResponse(Object response) {
        return CPAtomicLongCompareAndSetCodec.encodeResponse((Boolean) response);
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
