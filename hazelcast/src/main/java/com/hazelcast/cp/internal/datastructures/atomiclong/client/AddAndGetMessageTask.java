/*
 * Copyright (c) 2008-2020, Hazelcast, Inc. All Rights Reserved.
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
import com.hazelcast.client.impl.protocol.codec.CPAtomicLongAddAndGetCodec;
import com.hazelcast.client.impl.protocol.codec.CPAtomicLongAddAndGetCodec.RequestParameters;
import com.hazelcast.cp.internal.client.AbstractCPMessageTask;
import com.hazelcast.cp.internal.datastructures.atomiclong.RaftAtomicLongService;
import com.hazelcast.cp.internal.datastructures.atomiclong.operation.AddAndGetOp;
import com.hazelcast.instance.Node;
import com.hazelcast.nio.Connection;
import com.hazelcast.security.permission.ActionConstants;
import com.hazelcast.security.permission.AtomicLongPermission;

import java.security.Permission;

/**
 * Client message task for {@link AddAndGetOp}
 */
public class AddAndGetMessageTask extends AbstractCPMessageTask<RequestParameters> {

    public AddAndGetMessageTask(ClientMessage clientMessage, Node node, Connection connection) {
        super(clientMessage, node, connection);
    }

    @Override
    protected void processMessage() {
        invoke(parameters.groupId, new AddAndGetOp(parameters.name, parameters.delta));
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
        return "addAndGet";
    }

    @Override
    public Object[] getParameters() {
        return new Object[]{parameters.delta};
    }

    @Override
    protected CPAtomicLongAddAndGetCodec.RequestParameters decodeClientMessage(ClientMessage clientMessage) {
        return CPAtomicLongAddAndGetCodec.decodeRequest(clientMessage);
    }

    @Override
    protected ClientMessage encodeResponse(Object response) {
        return CPAtomicLongAddAndGetCodec.encodeResponse((Long) response);
    }
}
