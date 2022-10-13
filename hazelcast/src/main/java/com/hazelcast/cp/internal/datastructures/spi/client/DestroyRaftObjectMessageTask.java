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

package com.hazelcast.cp.internal.datastructures.spi.client;

import com.hazelcast.client.impl.protocol.ClientMessage;
import com.hazelcast.client.impl.protocol.codec.CPGroupDestroyCPObjectCodec;
import com.hazelcast.cp.internal.client.AbstractCPMessageTask;
import com.hazelcast.cp.internal.datastructures.spi.operation.DestroyRaftObjectOp;
import com.hazelcast.instance.impl.Node;
import com.hazelcast.internal.nio.Connection;
import com.hazelcast.security.permission.ActionConstants;

import java.security.Permission;

import static com.hazelcast.security.permission.ActionConstants.getPermission;

/**
 * Client message task for destroying Raft objects
 */
public class DestroyRaftObjectMessageTask extends AbstractCPMessageTask<CPGroupDestroyCPObjectCodec.RequestParameters> {

    public DestroyRaftObjectMessageTask(ClientMessage clientMessage, Node node, Connection connection) {
        super(clientMessage, node, connection);
    }

    @Override
    protected void processMessage() {
        invoke(parameters.groupId, new DestroyRaftObjectOp(parameters.serviceName, parameters.objectName));
    }

    @Override
    protected CPGroupDestroyCPObjectCodec.RequestParameters decodeClientMessage(ClientMessage clientMessage) {
        return CPGroupDestroyCPObjectCodec.decodeRequest(clientMessage);
    }

    @Override
    protected ClientMessage encodeResponse(Object response) {
        return CPGroupDestroyCPObjectCodec.encodeResponse();
    }

    @Override
    public String getServiceName() {
        return parameters.serviceName;
    }

    @Override
    public String getDistributedObjectName() {
        return parameters.objectName;
    }

    @Override
    public Permission getRequiredPermission() {
        return getPermission(parameters.objectName, parameters.serviceName, ActionConstants.ACTION_DESTROY);
    }

    @Override
    public String getMethodName() {
        return "destroyRaftObject";
    }

    @Override
    public Object[] getParameters() {
        return new Object[] {parameters.serviceName, parameters.objectName};
    }
}

