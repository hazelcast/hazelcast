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

package com.hazelcast.cp.internal.datastructures.countdownlatch.client;

import com.hazelcast.client.impl.protocol.ClientMessage;
import com.hazelcast.client.impl.protocol.codec.CountDownLatchGetRoundCodec;
import com.hazelcast.client.impl.protocol.task.AbstractMessageTask;
import com.hazelcast.core.ExecutionCallback;
import com.hazelcast.cp.internal.RaftService;
import com.hazelcast.cp.internal.datastructures.countdownlatch.CountDownLatchService;
import com.hazelcast.cp.internal.datastructures.countdownlatch.operation.GetCountOp;
import com.hazelcast.instance.impl.Node;
import com.hazelcast.nio.Connection;
import com.hazelcast.security.permission.ActionConstants;
import com.hazelcast.security.permission.CountDownLatchPermission;

import java.security.Permission;

import static com.hazelcast.cp.internal.raft.QueryPolicy.LINEARIZABLE;

/**
 * Client message task for {@link GetCountOp}
 */
public class GetCountMessageTask extends AbstractMessageTask<CountDownLatchGetRoundCodec.RequestParameters>
        implements ExecutionCallback<Integer> {

    public GetCountMessageTask(ClientMessage clientMessage, Node node, Connection connection) {
        super(clientMessage, node, connection);
    }

    @Override
    protected void processMessage() {
        RaftService service = nodeEngine.getService(RaftService.SERVICE_NAME);
        service.getInvocationManager()
               .<Integer>query(parameters.groupId, new GetCountOp(parameters.name), LINEARIZABLE)
               .andThen(this);
    }

    @Override
    protected CountDownLatchGetRoundCodec.RequestParameters decodeClientMessage(ClientMessage clientMessage) {
        return CountDownLatchGetRoundCodec.decodeRequest(clientMessage);
    }

    @Override
    protected ClientMessage encodeResponse(Object response) {
        return CountDownLatchGetRoundCodec.encodeResponse((Integer) response);
    }

    @Override
    public String getServiceName() {
        return CountDownLatchService.SERVICE_NAME;
    }

    @Override
    public Permission getRequiredPermission() {
        return new CountDownLatchPermission(parameters.name, ActionConstants.ACTION_READ);
    }

    @Override
    public String getDistributedObjectName() {
        return parameters.name;
    }

    @Override
    public String getMethodName() {
        return "getCount";
    }

    @Override
    public Object[] getParameters() {
        return new Object[0];
    }

    @Override
    public void onResponse(Integer response) {
        sendResponse(response);
    }

    @Override
    public void onFailure(Throwable t) {
        handleProcessingFailure(t);
    }
}
